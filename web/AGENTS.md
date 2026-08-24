# graphene/web — Frontend Guidelines

Vite + React 19 + TypeScript 7 SPA — web UI for Graphene CI (pipelines, runs,
logs, observability). Tailwind v4 + shadcn/ui (Radix), yarn.

## Layout (layered)

```
src/
├── components/    # React components
│   ├── ui/        # shadcn kit — domain-free primitives ONLY
│   ├── <Shared>.tsx  # single reusable component (flat PascalCase)
│   ├── <family>/  # reusable families of domain widgets: status/, logs/, ...
│   └── <feature>/ # feature cascades: runs/, pipelines/, settings/, ...
├── pages/         # THIN route files: feature composition + routing, no own logic
├── hooks/         # shared React hooks (use*)
├── stores/        # nanostores atoms/maps (persistent*, theme/lang); no React, no JSX
├── helpers/       # pure utilities — no React, no side effects, no I/O
├── lib/           # framework-agnostic infrastructure: API clients,
│                  # storage adapters, env readers, utils.ts (cn)
└── types/         # ambient .d.ts
```

Layer rules:

- `helpers/` — pure functions only. Imports nothing above it.
- `stores/` — nanostores; may import `helpers/` and `lib/`. No React, no
  JSX, no DOM side effects — store→document/i18n wiring lives in the
  composition root (`main.tsx`).
- `lib/` — client singletons/config, browser-API adapters. No React, no JSX,
  no hooks. If a module imports React, renders JSX, or calls hooks it does
  NOT belong here: stateful-via-React → `hooks/`, rendering → `components/`.
- `hooks/` — React state/effects over stores and lib.
- `components/` — rendering. Views read stores/hooks; **no RPC/fetch calls
  from .tsx** — data flows through API clients wired in `lib/`.
- `pages/` — thin route composition of components. A page file wires feature
  components to the router (guards, redirects, step switching) and renders —
  nothing else. No page-scoped component folders inside `pages/`.

## Feature structure inside `components/` (cascades)

- **Feature folder per domain area**, cascade by sub-feature:
  `components/runs/list/RunListItem.tsx`,
  `components/runs/detail/StepTimeline.tsx`. Max depth 3
  (`<feature>/<subfeature>/File.tsx`) — deeper means the feature needs
  splitting.
- **Shared components** — three tiers by nature:
  1. `components/ui/` — domain-free primitives (shadcn kit): zero knowledge
     of Pipeline/Run/API types.
  2. `components/<Shared>.tsx` — a **single** reusable component: flat
     PascalCase file in the root.
  3. `components/<family>/` — a reusable **family** of domain-aware widgets:
     its own root folder, e.g. `components/status/{RunStatusBadge,
     StepStatusIcon}.tsx`. A family differs from a feature only in meaning:
     feature = product area (runs, pipelines), family = domain widget
     imported by many features. Families never import features.
  A component private to one parent colocates in that parent's folder; the
  moment a second feature needs it — promote it up (feature root → shared
  tier above).
- **Import direction is one-way**: feature → `components/ui` / shared root /
  `hooks` / `stores` / `helpers` / `lib`. **Feature → feature is forbidden** —
  extract the shared piece or compose the two features at the page level.
- Feature-local non-JSX helpers may live inside the feature folder next to
  their consumers.

## Path aliases (always use, never relative parent walks)

```typescript
@/*  → src/   (@/components/…, @/hooks/…, @/lib/…, @/App)
```

Declared in `tsconfig.json` + `tsconfig.app.json` + `vite.config.ts` +
`components.json` — keep them in sync when changing the scheme. No `../../`
walks up the tree — alias only.

## shadcn/ui kit (`src/components/ui`)

Domain-free primitives only — zero product knowledge in `ui/`.

- **Add primitives only through the official CLI** from `graphene/web`:
  `npx shadcn@latest add <component>` (`components.json` drives
  paths/aliases). Never hand-write or copy-paste shadcn component source.
  Customizing tokens/styling after the CLI generates a component is fine.
- **After every CLI add, verify it didn't clobber existing styling**: the CLI
  silently overwrites files it pulls in as deps. `git diff src/components/ui`
  and restore any customized classNames/tokens the install reverted before
  committing.
- File naming: `ui/*.tsx` stay in shadcn's kebab-case (the CLI generates them
  that way). Everything the CLI drops **outside** `ui/` is ours and follows
  our convention — e.g. a registry hook lands as `hooks/use-mobile.ts`;
  rename it to `hooks/useIsMobile.ts` and fix imports right after the add.

## Coding style & naming

- TypeScript, React 19, JSX runtime. Functional components, hooks, explicit
  types. No `any` without explicit discussion.
- **Component and page files PascalCase** (`RunListItem.tsx`,
  `RunsPage.tsx`); hook files camelCase (`useRunStream.ts`);
  helpers/constants lowercase. Exported components PascalCase.
- Event handlers `handle*`; boolean state `is`/`has`/`should` prefixed.
- **No barrel files** — re-export barrels hurt Vite tree-shaking.
- Colocate: a helper component used **nowhere else** lives in its owner's
  feature folder; promote it up once it is used in more than one place.
- Avoid `renderX()` methods — extract self-contained UI into components.
  Too many props → split or compose (children/slots).
- Dead code is deleted, not deprecated.

## Data layer

All backend data flows through **connect-es** clients wired in `lib/api.ts`
(graphene-server, Connect protocol). Views read stores/hooks; mutations go
through `api.<service>.*` facades. Never open transports or call RPC/fetch
from a `.tsx` file.

- **Generated contracts** live in `src/proto/` (`*_pb.ts`) — produced by
  `make generate` in `graphene/` (easyp + `protoc-gen-es`, config in
  `easyp.yaml`). Never hand-edit; change the `.proto` and regenerate. Go
  and TS contracts regenerate in the same change.
- Dev: Vite proxies `/graphene.management.v1.*` to the server (`:7233` by
  default, `VITE_PROXY_TARGET` to override); production serves the UI
  same-origin behind the same reverse proxy. A detached backend uses
  `VITE_API_URL`.

## Forms (react-hook-form + zod)

Every form MUST use **react-hook-form + zod + `@hookform/resolvers/zod` +
shadcn Field primitives + RHF `Controller`** — no hand-rolled forms
(per-field `useState`, manual error strings, ad-hoc validation).

- One `Controller` per field; spread `{...field}` onto the control.
- `defaultValues` for every field — never `undefined`.
- Validation lives in zod only; colocate the schema with the form, derive
  `type Values = z.infer<typeof schema>`, export the schema.
- Server failures map to `form.setError('field'|'root', …)`; loading state is
  `form.formState.isSubmitting` — no separate `isSaving`.
- Accessibility: `data-invalid={fieldState.invalid}` on `<Field>`,
  `aria-invalid` on the control, `<FieldLabel htmlFor>` matching control `id`.
- Field arrays via `useFieldArray` keyed by `field.id`.

### Validation lifecycle — server parity + "reward early, punish late"

1. **The server is the source of truth; the client MIRRORS its contract.**
   Limits and regexes in the zod schema come from the server-side rules
   (validation in `graphene/proto` / manifest schemas) and are declared as
   named constants with a comment pointing at the source field. Client/server
   drift is a BUG with two failure modes: stricter than the server — valid
   input never passes; looser — the user hits an opaque server rejection
   after submit. Changing a server rule updates the client constant in the
   same change.
2. **Timing — reward early, punish late** (`mode: 'onTouched'`, RHF
   re-validates onChange after the first error): initial typing is NOT
   nagged per keystroke — the error appears on blur of a non-empty field; a
   field already in error clears immediately, on every input.
3. **Error text = what's wrong + how to fix**, with an example where the
   format is non-obvious. Never "Invalid input".
4. **Server errors map to a FIELD**: an attributable cause →
   `setError('<field>', …)` with a clear message. `root` is only for the
   non-attributable (network, unknown); on rejection the input is NOT lost,
   the form stays editable.
5. **Async availability** (unique name etc.): debounce ~400ms, race-guard by
   seq, starts ONLY after the format is valid; states visible at the field
   (idle / checking… / available ✓ / taken ✗). The check is a hint; the
   final truth is the mutating RPC itself.
6. **Submit is not disabled for invalid** static fields — submit surfaces
   all errors and focuses the first invalid field (RHF `shouldFocusError`).
   Disable only on `isSubmitting` and unfinished async gates with a visible
   reason at the field.
7. **Success is visible**: close/navigate/toast — the form never "silently
   works".
8. **Input normalization** — uniform rules for all forms:
   - edges are ALWAYS stripped (`z.string().trim()`);
   - name-like fields additionally collapse internal whitespace runs to one
     (`.transform(v => v.replace(/\s+/g, ' '))`);
   - free-text keeps its interior — only edge trim;
   - identifiers are normalized tolerantly to human input BEFORE format
     validation.
9. **Shared validator — single source.** The rule for a reusable field lives
   in ONE module and is imported by every form. The validator returns an
   attributable failure reason (not a boolean `valid`); fixing the rule
   fixes all forms at once.
10. **Blur autosave** (single settings field, inline) obeys the same
    contract as an explicit form; only the trigger differs. A form of TWO or
    more fields is always an explicit submit, never a scatter of autosaves.

## i18n — every user-facing string is translated (ru + en)

No hardcoded user-facing strings in components — ever, starting from the
first component. All copy goes through the i18n layer (i18next +
react-i18next; locales in `src/locales/{ru,en}/*.json`).

- Key path: `t('graphene.<area>.<...>')`; shared generics in `common.*`.
- React → `useTranslation()`; non-React modules → `i18n.t(...)`.
- Both `ru` and `en` land in the same change, identical key structure.
  Russian plurals need `_few`/`_many` on top of `_one`/`_other`.
- Translatable: JSX text, `placeholder`, `title`, `aria-label`, `alt`,
  visible labels/tooltips, toasts/confirms/errors. Not translatable:
  `console.*`, test ids, classNames, enum/API names, URLs, proper nouns.
- zod messages localize: build the schema in the component with
  `useMemo(() => z.object({...t(...)}), [t])`.

## Theming — colors only via theme tokens

6 themes — 3 light (`light` = GitLab light, `snow`, `paper`) + 3 dark
(`dark` = GitLab dark, `midnight`, `graphite`) — as classes on `<html>`
set from `@/stores/themeStore` (nanostores, persisted; wiring in
`main.tsx`); tokens live in `src/index.css` and flip per theme. The base
palette is **GitLab Pajamas**; `dark`/`light` are the primary pair,
`dark` is the default. All themes stay strict/austere — no playful
palettes. Token values carry a comment with the scale name — keep it when
changing a value. Light themes inherit the `:root` (light) status ramp;
dark themes share one common block (`.dark, .midnight, .graphite` —
status ramp, charts, scrim); per-theme blocks override surfaces/accents
only. A new theme = a new class in `index.css` + an entry in `THEMES`
(+ the dark-common selector and `@custom-variant dark` list if dark).

**`dark:` matches every dark theme class** (see `@custom-variant` in
`index.css`) but stays non-color-only — colors flip via tokens.

**Hard rule: no fractional and no inline colors ever reach the markup.**
A `.tsx`/component-level CSS file contains color ONLY as a semantic token
utility (`bg-card`, `text-muted-foreground`, …). Neither a literal
(`#…`/`rgb()`/`oklch()`/named/palette class) nor a fraction of a token
(`/10`, `/50`, `/[0.85]`) is allowed in markup — both are reviewed as bugs.

- **Never a literal color for surface/text/border**: no `bg-[#…]`,
  `bg-white`, `text-white`, `bg-slate-800`, no inline `style={{ color/
  background }}` with a color value, no literal colors inside arbitrary
  values (`shadow-[…rgba(…)]`, `ring-[#…]`). Use semantic utilities:
  `bg-background`, `bg-card`, `bg-popover`, `bg-muted`, `bg-primary`,
  `text-foreground`, `text-muted-foreground`, `border-border`,
  `ring-ring`, …
- **`dark:` variants are not for color** — color must flip via the token
  itself; `dark:` is acceptable only for non-color differences.
- **No ad-hoc color derivation in component code.** Derived shades are the
  same leak as literals.
  - Forbidden in components/inline styles: `color-mix(...)`,
    `oklch(from …)`, `filter: brightness/saturate` for repainting UI chrome,
    and **any alpha-slash on a token** — `bg-primary/10`,
    `border-foreground/50`, `text-foreground/[0.85]` and the like.
  - Need a translucent/derived shade (hover darkening, elevated surface,
    muted border)? That is a NEW token in `index.css`, derived there once —
    not a slash at the call site.
  - `color-mix`/`oklch(from …)`/alpha are allowed ONLY inside `index.css`
    when defining the tokens themselves.
- No exceptions via literals: scrims over media are a token
  (`bg-scrim`, defined once in `index.css`); CI status colors
  (success / failed / running / pending / warning / canceled / skipped)
  are tokens (`--status-*`, soft `--status-*-bg` for badges/rows) exposed
  as utilities and consumed only through the pinned module in
  `components/status/` — never raw `emerald`/`red`/`amber` classes at call
  sites. Hover/derived shades exist as tokens too (`--primary-hover`,
  `--surface-hover`, `--link`) — extend the set in `index.css` when a new
  shade is needed.
- Verify: grep changed files for literal colors AND derivation/alpha-slashes
  (`color-mix|oklch\(from|/\[0?\.|(bg|text|border|ring)-[a-z-]+/[0-9]`);
  check light + dark.

## Borders — minimal use

A border is the last separation tool, not the first. Surfaces are separated
by **background** (`bg-muted`/`bg-card` over the base), **spacing**, and
(for floating layers) **shadow** — not by frames.

- Do NOT draw a border around container blocks (lists, panels, field
  groups) "for decoration" — the surface is defined by its background.
- Divider lines (`border-t/b/r`) between zones — only when background
  separation is impossible (two zones on one surface with internal scroll).
- Legitimate exceptions: focus `ring-ring`, `border-input` on standalone kit
  inputs, `aria-invalid` highlighting.
- Check: see a `border` in new code — first ask "can background or spacing
  do this?". Usually it can.

## Typography — sizes only via the scalable token scale

All `--text-*` tokens are defined in `src/index.css` as
`calc(<rem> * var(--text-scale))` so a user text-size setting can scale the
whole app by writing `--text-scale` (default `1`). Rungs: `text-4xs`(8) →
`text-3xs`(10) → `text-2xs`(11) → `text-xs`(12) → `text-sm`(14, body
default) → … → `text-6xl`. Extend the scale in `index.css`, never inline.

- **Never arbitrary font sizes** — no `text-[11px]`, `[font-size:…]`, inline
  `fontSize`. They bypass the scale. Pick the nearest rung or add a token.
- Line-heights are paired per token and scale too — no `leading-[Npx]`.
- Font families only via tokens/utilities — no ad-hoc `font-family` or
  one-off font imports in components. `font-sans` = Geist Variable (UI),
  `font-mono` = JetBrains Mono Variable (logs, ids, hashes, code — never
  body text).

## Spacing — only the Tailwind scale

- Paddings/margins/gaps only via scale utilities (`p-2`, `gap-4`,
  `space-y-6`, …) — **no arbitrary values** (`p-[13px]`, `mt-[7px]`,
  inline `style={{ margin }}`). An off-scale need means the design is
  wrong or the scale needs a token — decide in `index.css`, not at the
  call site.
- Separation between blocks is spacing first (see Borders), and spacing is
  consistent: one rung inside a group, a bigger rung between groups.
- Fixed panel/layout dimensions (sidebar width etc.) are shared constants
  or CSS variables in `index.css` — not magic numbers repeated per file.

## Adaptive design — usable on ANY device (320px phone → ultra-wide)

Not "a desktop that shrinks tolerably", but one product, correct on every
device class.

### Window size classes

| Class | Width | Tailwind | Layout |
|---|---|---|---|
| compact | < 640px | base | one panel, stack navigation (list → detail) |
| medium | 640–1024px | `sm:`/`md:` | two panels, right panel as overlay |
| expanded | ≥ 1024px | `lg:`+ | panels inline |
| ultra-wide | ≥ 1920px | `2xl:` | panels grow to max-width, then air; content does NOT stretch |

- **Mobile-first**: base styles = compact, `sm:`/`md:`/`lg:` add on top.
- **Component-level responsiveness**: a component living inside a panel
  adapts via container queries (`@container`), not viewport media queries.
- Readability on wide screens: text line length ≤ ~75ch, lists/grids capped
  with `max-w-*`, surplus space centered.

### Input modality, not screen size

- **`@media (hover: hover) and (pointer: fine)`** — hover-only affordances
  live only under it. On `(pointer: coarse)` the same actions are available
  without hover: buttons always visible or reachable via long-press/menu.
- **Right-click ↔ long-press**: context actions go through one abstraction —
  right-click on fine pointer, long-press (~500ms, cancelled on scroll) on
  coarse. Never `onContextMenu` alone.
- No actions available only via double-click or only via hover.
- Touch feedback: `:active` states are mandatory.

### Touch targets

- Minimum **44×44px** (48 preferred) on coarse pointer, ≥ 8px gap between
  neighbors. Extend the hit area with padding/pseudo-element, not by
  growing the icon.
- Inputs on mobile — font ≥ 16px (otherwise iOS Safari zooms on focus).

### Overlays: modals, menus, popovers

- **compact**: dialogs = full-screen sheets or bottom-sheets with their OWN
  explicit close button and swipe-down dismiss; dropdown menus =
  bottom-sheets.
- **≥ medium**: centered dialogs, popovers at the anchor, Esc closes.
- One component — two renders (drawer + dialog), chosen by size class, NOT
  two different call sites.

### History and "back" (Android back = semantic back)

- Every dismissable layer (modal, sheet, panel, open detail on compact)
  **pushes a history state**; `popstate` closes the top layer. Back NEVER
  exits the app while there is somewhere to go back to.
- Closing by other means (close button, scrim tap, Esc) must clean up its
  phantom state (`history.back()` while the state is live) — otherwise back
  "fires blank".
- Screen states are deep-linkable: the route reflects the open
  entity/panel.

### Gestures and scroll

- Scroll containers: `overscroll-behavior: contain`; scroll/touch listeners
  are passive.
- Do not intercept the system edge-swipe; swipe gestures attach inside
  content.
- `user-select: none` only on purely interactive chrome — text (logs, ids)
  must stay selectable.

### Viewport, keyboard, cutouts

- Height — **`dvh`/`svh`, never `100vh`**.
- On-screen keyboard: inputs respect `visualViewport`, the input field is
  never covered by the keyboard.
- Safe-area: `env(safe-area-inset-*)` on edge-hugging elements;
  `viewport-fit=cover`.
- Horizontal body scroll does not exist at any width.

### Performance and preferences

- Long lists (runs, logs) — virtualization only; media — lazy.
- `prefers-reduced-motion` is respected: animations are muted/simplified.

### Adaptive verification gate

A UI change is done when verified on: compact (~375px, touch), medium
(~768px), expanded (~1280px), ultra-wide (~2560px); back semantics clicked
through with overlays open; keyboard — on a mobile viewport.

## Focus management — mandatory from the first component

Focus is application state, managed deliberately (W3C APG). Radix
primitives in `ui/` provide trap/restore out of the box — do NOT break
them; everything custom must behave the same.

- **Opening a layer**: focus moves INSIDE the layer (first meaningful
  element or the container with `tabindex="-1"`), the tree underneath is
  inert.
- **Trap**: a modal layer cycles Tab inside itself. Non-modal popovers
  don't trap but close on blur-out/Esc.
- **Restore**: closing a layer ALWAYS returns focus to the trigger (or an
  explicitly chosen element if the trigger is gone — focus the neighbor,
  not body).
- **Programmatic transitions**: switching a panel/screen moves focus to a
  meaningful target in the new context — focus never "dies" on a removed
  node and never sits on something invisible.
- **Visibility**: a `:focus-visible` ring (`ring-ring`) is mandatory on
  every interactive element; silencing outline without a replacement is
  forbidden. Mouse clicks don't draw the ring (focus-visible, not focus).
- **Composites** (run lists, tables): roving tabindex — the composite
  occupies ONE position in Tab order, arrows navigate inside; on re-entry
  focus lands on the last active element.
- Hidden/disabled is unreachable by Tab; `tabindex` > 0 is always
  forbidden.

## Keyboard support — full operability (WCAG 2.1.1)

Everything doable with mouse/touch is doable with the keyboard. A new
component without keyboard behavior is not done.

- **Base vocabulary (APG)**: Tab/Shift+Tab — between widgets; arrows —
  inside a widget; Enter/Space — activate; Esc — close top layer/cancel;
  Home/End — to the edges; PageUp/PageDown — long feeds.
- **No keyboard trap (WCAG 2.1.2)**: Esc is mandatory for every dismissable
  layer (the layer stack is shared by all dismissal methods).
- **App shortcuts**: registered centrally (a registry in `lib/`, not
  scattered `onKeyDown`), never shadow system/browser combos, never fire
  while focus is in a text field, and single-letter ones satisfy WCAG
  2.1.4.
- **Custom widgets**: an interactive element = native `button`/`a`/input,
  or a role + the FULL keyboard contract of that role per its APG pattern.
- `div`/`span` with onClick and no role, tabindex, and key handling —
  forbidden.
- **Gate**: new UI is clicked through WITHOUT a mouse from entry to result;
  visible focus at every step; Esc closes the same thing the close button
  does.

## Validation gate

`yarn build` (tsc + vite) must stay green.
