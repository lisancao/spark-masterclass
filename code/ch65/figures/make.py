"""Chapter 65 figures. Tokens from code/ch65/output (tokenizers), slot counts from the HashingTF output."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.figkit import Fig  # noqa: E402

CHECK = Path(sys.argv[1]) if len(sys.argv) > 1 else None


def chips(f, x, y, toks, removed=(), c="si"):
    for t in toks:
        w = 12 + len(t) * 8.4
        gone = t in removed
        f.rect(x, y, w, 26, c="bad" if gone else c, fill=0.06 if gone else 0.18, rx=4, dash="3 3" if gone else None)
        f.text(x + 6, y + 18, t, size=13, mono=True, c="mute" if gone else "ink")
        if gone:
            f.line(x + 3, y + 13, x + w - 3, y + 13, c="bad", w=1.4)
        x += w + 6
    return x


def tokens():
    f = Fig(900, 300, "The same sentence through two tokenizers and the stop word list")
    f.text(20, 26, 'Input: "Hello, order 1000 has a problem. Please help."', size=14, weight=700, mono=False)
    rows = [("Tokenizer", ['hello,', 'order', '1000', 'has', 'a', 'problem.', 'please', 'help.'], (), "gr",
             "white space only: the punctuation stays on the token"),
            ("RegexTokenizer \\W+", ['hello', 'order', '1000', 'has', 'a', 'problem', 'please', 'help'], (), "si",
             "cuts at each match of the pattern"),
            ("+ StopWordsRemover", ['hello', 'order', '1000', 'has', 'a', 'problem', 'please', 'help'], ("has", "a"), "si",
             "removes 'has' and 'a' (181 English stop words)"),
            ("gaps=False, \\d+", ['1000'], (), "go", "the pattern matches the tokens, not the gaps")]
    for i, (name, toks, rem, c, note) in enumerate(rows):
        y = 50 + i * 60
        f.text(20, y + 18, name, size=13, weight=600, mono=True)
        chips(f, 200, y, toks, rem, c)
        f.text(200, y + 44, note, size=12, c="mute")
    return f.save("ch65-tokens", CHECK)


def collisions():
    data = [("2^6", 64, 59, 68), ("2^8", 256, 100, 27), ("2^10", 1024, 119, 8), ("2^12", 4096, 125, 2),
            ("2^18", 262144, 127, 0)]
    f = Fig(900, 270, "Words that share a HashingTF slot, by numFeatures")
    f.text(20, 26, "127 distinct words in 400 tickets: words that share a slot", size=14, weight=700)
    X0, S = 230, 5.5
    for i, (p, n, slots, shared) in enumerate(data):
        y = 46 + i * 40
        f.text(20, y + 19, f"numFeatures {p}", size=13, mono=True, weight=600)
        f.text(X0 - 10, y + 19, f"{n:,}", size=12, mono=True, c="mute", anchor="end")
        if shared:
            f.rect(X0, y + 4, shared * S, 24, c="bad", fill=0.35, rx=3, stroke=0)
        f.text(X0 + shared * S + 8, y + 21, f"{shared} words share a slot ({slots} slots used)", size=12)
    f.text(20, 258, "A collision adds the counts of two words. 2^18 is the default.", size=12, c="mute")
    return f.save("ch65-collisions", CHECK)


if __name__ == "__main__":
    for fn in (tokens, collisions):
        print(fn())
