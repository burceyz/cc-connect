package discord

import (
	"strings"
	"testing"

	"github.com/chenhg5/cc-connect/core"
)

func TestPendingProgress_RenderNeutralLocalizedState(t *testing.T) {
	for _, lang := range []core.Language{core.LangEnglish, core.LangChinese, core.LangTraditionalChinese, core.LangJapanese, core.LangSpanish} {
		i18n := core.NewI18n(lang)
		embed := buildDiscordProgressEmbed(&core.ProgressCardPayload{State: core.ProgressCardStatePending, Lang: string(lang),
			Agent: "Test Agent", Items: []core.ProgressCardEntry{{Kind: core.ProgressEntryInfo, Text: "review submitted"}}})
		if embed.Color != 0x95A5A6 || !strings.Contains(embed.Title, i18n.T(core.MsgPendingTitle)) {
			t.Fatalf("pending embed is not neutral/localized (%s): %+v", lang, embed)
		}
		if embed.Footer == nil || embed.Footer.Text != i18n.T(core.MsgPendingFooter) {
			t.Fatalf("pending footer missing (%s): %+v", lang, embed)
		}
	}
}
