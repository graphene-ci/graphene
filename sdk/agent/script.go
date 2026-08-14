package agent

import (
	"fmt"
	"strings"
)

// Install describes one installation of the agent: where it should call
// home, what to call itself, and what proves it is allowed.
type Install struct {
	// Control is where the agent fetches its binary from.
	Control string
	// Address is the Temporal frontend the agent connects to.
	Address string
	// Namespace is Temporal's namespace.
	Namespace string
	// Records is the cluster namespace the machine's record goes into.
	Records string
	// Machine is what this installation calls itself. The queue follows
	// from it, which is why a step can be scheduled before the machine
	// exists.
	Machine string
	// Traces is where the agent sends what it saw, in OTLP.
	//
	// The machine is outside the cluster, so this is an address it can
	// actually reach — the same problem as the storage's, and the same
	// answer. Empty is a working configuration: then the agent records
	// nothing and costs nothing for it.
	Traces string
	// Token proves the installation was asked for.
	//
	// ДЫРКА, НАЗВАННАЯ ВСЛУХ: этот токен попадает в спеку ресурса
	// провайдера (user-data видно всякому, кто может читать ВМ) и в
	// историю Temporal. Пока он одноразовый только по договорённости, а
	// не по устройству. Закрывается после M8 — коротким сроком жизни и
	// обменом на настоящую учётку при первом подключении. Записано и
	// здесь, и в FORM.md, чтобы не выяснилось однажды на разборе.
	Token string
}

// Script is what a person or a cloud runs to put the agent on a machine.
//
// One script for both paths — `curl … | bash` on an existing machine and
// user-data of a fresh VM — because two would drift, and the one used less
// often would be the broken one.
//
// It must be a pure function of Install: the same installation asked for
// twice has to produce the same bytes, or Apply stops being idempotent for
// anything that carries it.
func (i Install) Script() string {
	var out strings.Builder

	out.WriteString("#!/bin/sh\nset -eu\n\n")
	out.WriteString("# Ставит агента graphene. Порождено системой; править смысла нет.\n")

	env(&out, "GRAPHENE_CONTROL", i.Control)
	env(&out, "GRAPHENE_TEMPORAL_ADDRESS", i.Address)
	env(&out, "GRAPHENE_TEMPORAL_NAMESPACE", i.Namespace)
	env(&out, "GRAPHENE_NAMESPACE", i.Records)
	env(&out, "GRAPHENE_MACHINE", i.Machine)
	env(&out, "OTEL_EXPORTER_OTLP_ENDPOINT", i.Traces)
	env(&out, "GRAPHENE_TOKEN", i.Token)

	out.WriteString(body)

	return out.String()
}

// CloudInit wraps the same script for a machine that does not exist yet.
func (i Install) CloudInit() string {
	var out strings.Builder

	out.WriteString("#cloud-config\nwrite_files:\n")
	out.WriteString("  - path: /opt/graphene/install.sh\n")
	out.WriteString("    permissions: '0700'\n")
	out.WriteString("    content: |\n")

	for line := range strings.SplitSeq(i.Script(), "\n") {
		out.WriteString("      " + line + "\n")
	}

	out.WriteString("runcmd:\n  - /opt/graphene/install.sh\n")

	return out.String()
}

func env(out *strings.Builder, name, value string) {
	fmt.Fprintf(out, "%s='%s'\nexport %s\n", name, strings.ReplaceAll(value, "'", `'\''`), name)
}

// body is the part that does not depend on the installation.
//
// The binary comes from the control plane rather than from a release page,
// and that is deliberate: then the agent's version always matches the
// system's, and there is one moving part instead of two.
const body = `
ARCH=$(uname -m)
case "$ARCH" in
  x86_64) ARCH=amd64 ;;
  aarch64) ARCH=arm64 ;;
esac

mkdir -p /opt/graphene
# Скачиваем рядом и переставляем: работающий агент держит свой бинарь, и
# запись поверх него отказывает "text file busy". Переустановка обязана
# проходить на машине, где агент уже стоит, — иначе это установка,
# работающая ровно один раз.
curl -fsSL "$GRAPHENE_CONTROL/agent/linux/$ARCH/graphene-agent" -o /opt/graphene/graphene-agent.new
chmod +x /opt/graphene/graphene-agent.new
mv -f /opt/graphene/graphene-agent.new /opt/graphene/graphene-agent

# Служба, если система умеет службы; иначе просто процесс. Второе — про
# контейнер, на котором эта механика и проверяется.
if command -v systemctl >/dev/null 2>&1; then
  cat > /etc/systemd/system/graphene-agent.service <<UNIT
[Unit]
Description=graphene agent
After=network-online.target

[Service]
ExecStart=/opt/graphene/graphene-agent
Environment=GRAPHENE_TEMPORAL_ADDRESS=$GRAPHENE_TEMPORAL_ADDRESS
Environment=GRAPHENE_TEMPORAL_NAMESPACE=$GRAPHENE_TEMPORAL_NAMESPACE
Environment=GRAPHENE_NAMESPACE=$GRAPHENE_NAMESPACE
Environment=GRAPHENE_MACHINE=$GRAPHENE_MACHINE
Environment=OTEL_EXPORTER_OTLP_ENDPOINT=$OTEL_EXPORTER_OTLP_ENDPOINT
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
UNIT
  systemctl daemon-reload
  systemctl enable graphene-agent
  # restart, а не enable --now: для уже работающей службы --now ничего не
  # делает, и переустановка молча не вступала бы в силу — новый бинарь
  # лежит, а работает старый процесс со старым окружением.
  systemctl restart graphene-agent
else
  /opt/graphene/graphene-agent
fi
`
