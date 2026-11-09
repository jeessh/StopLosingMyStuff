package capture

import (
	"regexp"
	"unicode/utf8"
)

const redacted = "[redacted]"

var secretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?(?:-----END [A-Z ]*PRIVATE KEY-----|$)`),
	regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9]{20,}`),
	regexp.MustCompile(`\bgithub_pat_[A-Za-z0-9_]{20,}`),
	regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{20,}`),
	regexp.MustCompile(`\bxox[abprs]-[A-Za-z0-9-]{10,}`),
	regexp.MustCompile(`\b[rs]k_(?:live|test)_[A-Za-z0-9]{10,}`),
	regexp.MustCompile(`\bnpm_[A-Za-z0-9]{20,}`),
	regexp.MustCompile(`\bglpat-[\w-]{10,}`),
	regexp.MustCompile(`\bA(?:KIA|SIA)[0-9A-Z]{16}\b`),
	regexp.MustCompile(`\bAIza[0-9A-Za-z_-]{35}\b`),
	regexp.MustCompile(`\beyJ[\w-]{10,}\.[\w-]{10,}\.[\w-]{10,}`),
}

var (
	assignment = regexp.MustCompile(`(?i)([\w.-]*(?:password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|auth[_-]?key|credential)[\w.-]*(?:\\?["'])?\s*[=:]\s*)(\\"[^"\\]*\\"|"[^"]*"|'[^']*'|[^\s,;&]+)`)
	envKey     = regexp.MustCompile(`\b([A-Z][A-Z0-9_]*(?:_KEY|_PASS|_PAT)=)("[^"]*"|'[^']*'|\S+)`)
	cliFlag    = regexp.MustCompile(`(?i)(\s--?(?:password|passwd|pass|token|api-?key|secret|auth-token)[= ]+)("[^"]*"|'[^']*'|\S+)`)
	sshpass    = regexp.MustCompile(`\b(sshpass\s+-p\s*)("[^"]*"|'[^']*'|\S+)`)
	dbClient   = regexp.MustCompile(`\b((?:mysql|mysqldump|mysqladmin)\b[^|;&\n]*?\s-p)([^\s-]\S*)`)
	userPass   = regexp.MustCompile(`(\s(?:-u|--user)[= ]+[^\s:]+:)(\S+)`)
	bearer     = regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}`)
	urlCreds   = regexp.MustCompile(`([a-z][a-z0-9+.-]*://[^/\s:@]+:)[^/\s@]+@`)
)

func Redact(s string) string {
	for _, re := range secretPatterns {
		s = re.ReplaceAllString(s, redacted)
	}
	s = assignment.ReplaceAllString(s, "${1}"+redacted)
	for _, re := range []*regexp.Regexp{envKey, cliFlag, sshpass, dbClient, userPass} {
		s = re.ReplaceAllString(s, "${1}"+redacted)
	}
	s = bearer.ReplaceAllString(s, "$1 "+redacted)
	return urlCreds.ReplaceAllString(s, "${1}"+redacted+"@")
}

// Clip runs after Redact so a cut can't turn a secret into something the
// patterns no longer match.
func Clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + "..."
}
