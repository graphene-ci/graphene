package ctl

import (
	"github.com/spf13/cobra"

	appctl "github.com/graphene-ci/graphene/internal/app/ctl"
)

// Completion asks the kernel the same questions an operator would: which
// kinds exist, what lives under a path, which fields a kind has. It is the
// live API, not a static list — a kind defined a minute ago completes.
//
// Every failure (no kernel, no token, timeout) yields no suggestions
// rather than an error: a tab press must stay instant and quiet.

func completeAddress(command *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	suggester, ok := suggesterFor(command)
	if !ok {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	switch len(args) {
	case 0:
		return suggester.Kinds(toComplete), cobra.ShellCompDirectiveNoFileComp
	case 1:
		// Path segments end in "/" while more segments may follow, so the
		// shell must not append a space after them.
		return suggester.Paths(args[0], toComplete),
			cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveNoSpace
	default:
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
}

// completeSelector suggests field paths from the kind's SCHEMA — the
// definition is part of the API, so the shell can offer exactly the fields
// this kind has.
func completeSelector(command *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	suggester, ok := suggesterFor(command)
	if !ok || len(args) == 0 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	return suggester.Fields(args[0], toComplete),
		cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveNoSpace
}

func completeFormat(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
	return []string{
		string(appctl.FormatYAML) + "\tcanonical exchange form (apply reads it back)",
		string(appctl.FormatJSON) + "\tsame content as json",
		string(appctl.FormatName) + "\taddresses only",
	}, cobra.ShellCompDirectiveNoFileComp
}

func completeYAMLFile(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
	return []string{"yaml", "yml"}, cobra.ShellCompDirectiveFilterFileExt
}

// suggesterFor builds a suggester from the connection flags already typed
// on the command line (and the environment token).
func suggesterFor(command *cobra.Command) (*appctl.Suggester, bool) {
	flags, err := newTargetFlags(command)
	if err != nil {
		return nil, false
	}

	if flags.Validate() != nil {
		return nil, false
	}

	return appctl.NewSuggester(flags.target()), true
}
