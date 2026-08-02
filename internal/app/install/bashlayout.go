package install

// bashOneColumn overrides cobra's description formatter so a tab press
// always lists one candidate per line.
//
// Readline decides the layout: it packs as many candidates per line as the
// terminal width allows, measuring the longest one. With descriptions
// attached, long lists (the root command) happen to fill a line and look
// like a column, while short ones (a subcommand group) get paired up — the
// same tab press looks different depending on what is being completed.
//
// Padding every candidate to the terminal width removes the choice from
// readline: two of them can never share a line. The override is appended
// after cobra's own definition, so the shell simply uses the last one and
// nothing in the generated script has to be edited.
// BashOneColumn is appended to the generated bash script; see above.
//
//nolint:dupword // shell syntax, not prose
const BashOneColumn = `

# Installed by graphen: one candidate per line, whatever their length.
__graphen_format_comp_descriptions()
{
    local tab=$'\t'
    local comp desc maxdesclength width
    local longest=$1

    # Fall back to a sane width when COLUMNS is unset (non-interactive).
    width=${COLUMNS:-80}

    local i ci
    for ci in ${!COMPREPLY[*]}; do
        comp=${COMPREPLY[ci]}
        if [[ "$comp" == *$tab* ]]; then
            desc=${comp#*$tab}
            comp=${comp%%$tab*}

            # Align the descriptions with each other.
            for ((i = ${#comp} ; i < longest ; i++)); do
                comp+=" "
            done

            maxdesclength=$(( width - longest - 6 ))
            if ((maxdesclength > 0)); then
                if ((${#desc} > maxdesclength)); then
                    desc=${desc:0:$(( maxdesclength - 1 ))}
                    desc+="…"
                fi
                comp+="  ($desc)"
            fi
        fi

        # Pad to the full width so readline cannot fit a second candidate
        # on the line.
        for ((i = ${#comp} ; i < width - 1 ; i++)); do
            comp+=" "
        done

        COMPREPLY[ci]=$comp
    done
}
`
