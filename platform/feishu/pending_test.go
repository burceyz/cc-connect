package feishu

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/chenhg5/cc-connect/core"
)

func TestPendingCards_RenderNeutralLocalizedTerminalState(t *testing.T) {
	for _, lang := range []core.Language{core.LangEnglish, core.LangChinese, core.LangTraditionalChinese, core.LangJapanese, core.LangSpanish} {
		i18n := core.NewI18n(lang)
		for _, rich := range []bool{false, true} {
			var content string
			if rich {
				content = buildRichCard(core.CardStatusPending, i18n.T(core.MsgPendingTitle), nil, i18n.T(core.MsgPendingReviewWaiting), false, "")
			} else {
				content = buildProgressCardJSONFromPayload(&core.ProgressCardPayload{State: core.ProgressCardStatePending, Lang: string(lang),
					Agent: "Test Agent", Items: []core.ProgressCardEntry{{Kind: core.ProgressEntryInfo, Text: "review submitted"}}})
			}
			var card map[string]any
			if err := json.Unmarshal([]byte(content), &card); err != nil {
				t.Fatal(err)
			}
			header := card["header"].(map[string]any)
			title := header["title"].(map[string]any)["content"].(string)
			if header["template"] != "grey" || !strings.Contains(title, i18n.T(core.MsgPendingTitle)) {
				t.Fatalf("pending card is not neutral/localized (%s, rich=%t): %s", lang, rich, content)
			}
			if rich && card["config"].(map[string]any)["streaming_mode"] != false {
				t.Fatalf("pending rich card kept its streaming animation: %s", content)
			}
		}
	}
}
