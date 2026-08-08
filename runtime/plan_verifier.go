package runtime

import (
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/SpekoAI/gateway/protocol"
)

const (
	// SessionPlanJWSType prevents a key intended for another control-plane
	// artifact from being used to authorize a provider connection.
	SessionPlanJWSType     = "speko.session-plan+jws"
	defaultJWKSCacheTTL    = 5 * time.Minute
	defaultMaxJWKSCacheTTL = time.Hour
	defaultClockSkew       = 30 * time.Second
	maxJWKSBytes           = 1 << 20
)

var (
	// ErrPlanReplay is returned when an otherwise-valid signed plan is used
	// more than once within its envelope lifetime.
	ErrPlanReplay = errors.New("runtime: signed session plan has already been used")
	// ErrPlanSignature is returned for malformed, tampered, or unsupported
	// compact JWS values. It never includes raw credentials or JWS contents.
	ErrPlanSignature = errors.New("runtime: invalid signed session plan")
)

// ReplayCache records verified envelope IDs until expiry. Production users
// that run several gateways should supply a shared, atomic implementation;
// MemoryReplayCache is safe within one runtime process and is the default.
type ReplayCache interface {
	TryUse(context.Context, string, time.Time) (bool, error)
}

// MemoryReplayCache is a bounded-by-expiry in-process replay cache.
type MemoryReplayCache struct {
	mu   sync.Mutex
	now  func() time.Time
	used map[string]time.Time
}

// NewMemoryReplayCache constructs an in-memory replay cache.
func NewMemoryReplayCache(now func() time.Time) *MemoryReplayCache {
	if now == nil {
		now = time.Now
	}
	return &MemoryReplayCache{now: now, used: make(map[string]time.Time)}
}

// TryUse atomically consumes ID until its expiry. It returns false when the ID
// was previously consumed and has not yet expired.
func (c *MemoryReplayCache) TryUse(_ context.Context, id string, expiresAt time.Time) (bool, error) {
	if strings.TrimSpace(id) == "" || expiresAt.IsZero() {
		return false, errors.New("runtime: replay cache requires an id and expiry")
	}
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	for usedID, expiry := range c.used {
		if !expiry.After(now) {
			delete(c.used, usedID)
		}
	}
	if expiry, exists := c.used[id]; exists && expiry.After(now) {
		return false, nil
	}
	c.used[id] = expiresAt
	return true, nil
}

// PlanVerifierConfig identifies the control-plane signing authority and local
// verification policy. JWKSURL must be HTTPS in production; the explicit
// AllowInsecureJWKS option exists only for hermetic HTTP test fixtures.
type PlanVerifierConfig struct {
	JWKSURL           string
	Issuer            string
	Audience          string
	HTTPClient        *http.Client
	CacheTTL          time.Duration
	MaxCacheTTL       time.Duration
	ClockSkew         time.Duration
	Now               func() time.Time
	ReplayCache       ReplayCache
	AllowInsecureJWKS bool
}

// JWKSPlanVerifier verifies compact JWS session-plan envelopes using cached
// control-plane public keys. An unknown key ID forces one synchronous refresh
// so normal signing-key rotation does not wait for the cache TTL to expire.
type JWKSPlanVerifier struct {
	config PlanVerifierConfig

	mu        sync.Mutex
	refreshMu sync.Mutex
	keys      map[string]jwkKey
	expiresAt time.Time
	etag      string
}

type jwksDocument struct {
	Keys []json.RawMessage `json:"keys"`
}

type jwkKey struct {
	ID        string
	Algorithm string
	PublicKey crypto.PublicKey
}

// NewPlanVerifier creates the production PlanVerifier implementation used by
// an embedded runtime or gateway.
func NewPlanVerifier(config PlanVerifierConfig) (*JWKSPlanVerifier, error) {
	if strings.TrimSpace(config.Issuer) == "" || strings.TrimSpace(config.Audience) == "" {
		return nil, errors.New("runtime: plan verifier issuer and audience are required")
	}
	endpoint, err := url.Parse(config.JWKSURL)
	if err != nil || endpoint.Host == "" || (endpoint.Scheme != "https" && !(config.AllowInsecureJWKS && endpoint.Scheme == "http")) {
		return nil, errors.New("runtime: plan verifier jwks_url must be an absolute https URL")
	}
	if config.CacheTTL == 0 {
		config.CacheTTL = defaultJWKSCacheTTL
	}
	if config.CacheTTL <= 0 {
		return nil, errors.New("runtime: plan verifier cache ttl must be positive")
	}
	if config.MaxCacheTTL == 0 {
		config.MaxCacheTTL = defaultMaxJWKSCacheTTL
	}
	if config.MaxCacheTTL <= 0 {
		return nil, errors.New("runtime: plan verifier maximum cache ttl must be positive")
	}
	if config.ClockSkew == 0 {
		config.ClockSkew = defaultClockSkew
	}
	if config.ClockSkew < 0 {
		return nil, errors.New("runtime: plan verifier clock skew cannot be negative")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.ReplayCache == nil {
		config.ReplayCache = NewMemoryReplayCache(config.Now)
	}
	if config.HTTPClient == nil {
		config.HTTPClient = http.DefaultClient
	}
	return &JWKSPlanVerifier{config: config, keys: make(map[string]jwkKey)}, nil
}

// Verify validates the outer plan, verifies its JWS, binds the envelope to
// that exact plan, validates issuer/audience/times, and consumes its jti.
func (v *JWKSPlanVerifier) Verify(ctx context.Context, plan protocol.SessionPlan) error {
	now := v.config.Now().UTC()
	if err := plan.Validate(now); err != nil {
		return fmt.Errorf("%w: plan validation: %v", ErrPlanSignature, err)
	}
	header, payload, signingInput, err := parseCompactJWS(plan.Signature)
	if err != nil {
		return err
	}
	key, err := v.keyFor(ctx, header.KeyID)
	if err != nil {
		return err
	}
	if key.Algorithm != "" && key.Algorithm != header.Algorithm {
		return fmt.Errorf("%w: jwks key algorithm does not match jws header", ErrPlanSignature)
	}
	if err := verifyJWS(key.PublicKey, header.Algorithm, signingInput, header.Signature); err != nil {
		return err
	}
	var envelope protocol.SessionPlanEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return fmt.Errorf("%w: envelope payload is not valid JSON", ErrPlanSignature)
	}
	if err := envelope.Validate(now, v.config.Issuer, v.config.Audience, v.config.ClockSkew); err != nil {
		return fmt.Errorf("%w: envelope validation: %v", ErrPlanSignature, err)
	}
	if err := planMatchesEnvelope(plan, envelope.Plan); err != nil {
		return err
	}
	accepted, err := v.config.ReplayCache.TryUse(ctx, envelope.ID, envelope.ExpiresAt.Add(v.config.ClockSkew))
	if err != nil {
		return fmt.Errorf("runtime: record session plan replay guard: %w", err)
	}
	if !accepted {
		return ErrPlanReplay
	}
	return nil
}

type compactJWSHeader struct {
	Algorithm  string   `json:"alg"`
	KeyID      string   `json:"kid"`
	Type       string   `json:"typ"`
	Critical   []string `json:"crit"`
	B64Payload *bool    `json:"b64"`
	Signature  []byte   `json:"-"`
}

func parseCompactJWS(compact string) (compactJWSHeader, []byte, []byte, error) {
	return parseCompactJWSWithType(compact, SessionPlanJWSType)
}

// parseCompactJWSWithType splits and vets a compact JWS whose protected
// header must carry exactly the expected typ. The typ is a parameter because
// session plans and relay plans share one JWS layout while staying mutually
// unacceptable: this header check is what stops a signature minted for one
// artifact from authorizing the other, even under a shared signing key.
func parseCompactJWSWithType(compact, expectedType string) (compactJWSHeader, []byte, []byte, error) {
	parts := strings.Split(compact, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return compactJWSHeader{}, nil, nil, fmt.Errorf("%w: expected compact serialization", ErrPlanSignature)
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return compactJWSHeader{}, nil, nil, fmt.Errorf("%w: header encoding", ErrPlanSignature)
	}
	var header compactJWSHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return compactJWSHeader{}, nil, nil, fmt.Errorf("%w: header JSON", ErrPlanSignature)
	}
	if header.Algorithm != "EdDSA" && header.Algorithm != "RS256" {
		return compactJWSHeader{}, nil, nil, fmt.Errorf("%w: unsupported algorithm", ErrPlanSignature)
	}
	if header.Type != expectedType || strings.TrimSpace(header.KeyID) == "" || len(header.Critical) != 0 || header.B64Payload != nil {
		return compactJWSHeader{}, nil, nil, fmt.Errorf("%w: unsafe or incomplete protected header", ErrPlanSignature)
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return compactJWSHeader{}, nil, nil, fmt.Errorf("%w: payload encoding", ErrPlanSignature)
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return compactJWSHeader{}, nil, nil, fmt.Errorf("%w: signature encoding", ErrPlanSignature)
	}
	header.Signature = signature
	return header, payload, []byte(parts[0] + "." + parts[1]), nil
}

func verifyJWS(publicKey crypto.PublicKey, algorithm string, signingInput, signature []byte) error {
	switch algorithm {
	case "EdDSA":
		key, ok := publicKey.(ed25519.PublicKey)
		if !ok || !ed25519.Verify(key, signingInput, signature) {
			return fmt.Errorf("%w: ed25519 verification failed", ErrPlanSignature)
		}
	case "RS256":
		key, ok := publicKey.(*rsa.PublicKey)
		if !ok {
			return fmt.Errorf("%w: rsa key required for RS256", ErrPlanSignature)
		}
		digest := sha256.Sum256(signingInput)
		if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature); err != nil {
			return fmt.Errorf("%w: rsa verification failed", ErrPlanSignature)
		}
	default:
		// parseCompactJWSWithType allowlists exactly the algorithms this
		// switch verifies, so this arm is unreachable today. It fails closed
		// anyway: extending the allowlist without teaching this switch the
		// new algorithm must reject every JWS claiming it, not fall through
		// and accept any signature under the new name.
		return fmt.Errorf("%w: unsupported algorithm", ErrPlanSignature)
	}
	return nil
}

func planMatchesEnvelope(plan, enclosed protocol.SessionPlan) error {
	want, err := json.Marshal(plan.Unsigned())
	if err != nil {
		return fmt.Errorf("%w: encode plan", ErrPlanSignature)
	}
	got, err := json.Marshal(enclosed)
	if err != nil {
		return fmt.Errorf("%w: encode envelope plan", ErrPlanSignature)
	}
	if string(want) != string(got) {
		return fmt.Errorf("%w: envelope does not bind the supplied plan", ErrPlanSignature)
	}
	return nil
}

func (v *JWKSPlanVerifier) keyFor(ctx context.Context, keyID string) (jwkKey, error) {
	now := v.config.Now()
	v.mu.Lock()
	key, found := v.keys[keyID]
	fresh := now.Before(v.expiresAt)
	v.mu.Unlock()
	if found && fresh {
		return key, nil
	}
	if err := v.refresh(ctx, !found); err != nil {
		return jwkKey{}, err
	}
	v.mu.Lock()
	key, found = v.keys[keyID]
	v.mu.Unlock()
	if !found {
		return jwkKey{}, fmt.Errorf("%w: signing key is not present in jwks", ErrPlanSignature)
	}
	return key, nil
}

func (v *JWKSPlanVerifier) refresh(ctx context.Context, force bool) error {
	v.refreshMu.Lock()
	defer v.refreshMu.Unlock()

	// Another caller could have completed a forced key-rotation refresh while
	// this caller waited. A fresh cache always has a complete key set.
	v.mu.Lock()
	if !force && v.config.Now().Before(v.expiresAt) && len(v.keys) != 0 {
		v.mu.Unlock()
		return nil
	}
	etag := v.etag
	v.mu.Unlock()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, v.config.JWKSURL, nil)
	if err != nil {
		return fmt.Errorf("runtime: create jwks request: %w", err)
	}
	request.Header.Set("Accept", "application/jwk-set+json, application/json")
	if etag != "" {
		request.Header.Set("If-None-Match", etag)
	}
	response, err := v.config.HTTPClient.Do(request)
	if err != nil {
		return fmt.Errorf("runtime: retrieve jwks: %w", err)
	}
	defer response.Body.Close()
	if response.Request == nil || response.Request.URL == nil || (response.Request.URL.Scheme != "https" && !(v.config.AllowInsecureJWKS && response.Request.URL.Scheme == "http")) {
		return errors.New("runtime: jwks redirect left the permitted URL scheme")
	}
	if response.StatusCode == http.StatusNotModified {
		v.mu.Lock()
		if len(v.keys) == 0 {
			v.mu.Unlock()
			return errors.New("runtime: control plane returned an empty cached jwks")
		}
		v.expiresAt = v.config.Now().Add(v.cacheTTL(response))
		v.mu.Unlock()
		return nil
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("runtime: retrieve jwks: unexpected HTTP status %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxJWKSBytes+1))
	if err != nil {
		return fmt.Errorf("runtime: read jwks: %w", err)
	}
	if len(body) > maxJWKSBytes {
		return errors.New("runtime: jwks response exceeds size limit")
	}
	var document jwksDocument
	if err := json.Unmarshal(body, &document); err != nil || len(document.Keys) == 0 {
		return errors.New("runtime: jwks response does not contain keys")
	}
	keys := make(map[string]jwkKey, len(document.Keys))
	for _, raw := range document.Keys {
		key, err := parseJWK(raw)
		if err != nil {
			return fmt.Errorf("runtime: parse jwks key: %w", err)
		}
		if _, exists := keys[key.ID]; exists {
			return errors.New("runtime: jwks contains duplicate key IDs")
		}
		keys[key.ID] = key
	}
	v.mu.Lock()
	v.keys = keys
	v.etag = response.Header.Get("ETag")
	v.expiresAt = v.config.Now().Add(v.cacheTTL(response))
	v.mu.Unlock()
	return nil
}

func (v *JWKSPlanVerifier) cacheTTL(response *http.Response) time.Duration {
	ttl := v.config.CacheTTL
	for _, directive := range strings.Split(response.Header.Get("Cache-Control"), ",") {
		parts := strings.SplitN(strings.TrimSpace(directive), "=", 2)
		if len(parts) != 2 || strings.ToLower(parts[0]) != "max-age" {
			continue
		}
		seconds, err := strconv.Atoi(strings.Trim(parts[1], `"`))
		if err == nil && seconds >= 0 {
			ttl = time.Duration(seconds) * time.Second
		}
	}
	if ttl <= 0 {
		ttl = time.Second
	}
	if ttl > v.config.MaxCacheTTL {
		return v.config.MaxCacheTTL
	}
	return ttl
}

func parseJWK(raw json.RawMessage) (jwkKey, error) {
	var document struct {
		KTY string `json:"kty"`
		KID string `json:"kid"`
		ALG string `json:"alg"`
		USE string `json:"use"`
		CRV string `json:"crv"`
		X   string `json:"x"`
		N   string `json:"n"`
		E   string `json:"e"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		return jwkKey{}, err
	}
	if strings.TrimSpace(document.KID) == "" || (document.ALG != "" && document.ALG != "EdDSA" && document.ALG != "RS256") || (document.USE != "" && document.USE != "sig") {
		return jwkKey{}, errors.New("unsupported jwks key metadata")
	}
	result := jwkKey{ID: document.KID, Algorithm: document.ALG}
	switch document.KTY {
	case "OKP":
		if document.CRV != "Ed25519" {
			return jwkKey{}, errors.New("unsupported OKP curve")
		}
		bytes, err := base64.RawURLEncoding.DecodeString(document.X)
		if err != nil || len(bytes) != ed25519.PublicKeySize {
			return jwkKey{}, errors.New("invalid Ed25519 public key")
		}
		result.PublicKey = ed25519.PublicKey(bytes)
	case "RSA":
		modulus, err := base64.RawURLEncoding.DecodeString(document.N)
		if err != nil || len(modulus) == 0 {
			return jwkKey{}, errors.New("invalid RSA modulus")
		}
		exponentBytes, err := base64.RawURLEncoding.DecodeString(document.E)
		if err != nil || len(exponentBytes) == 0 || len(exponentBytes) > 4 {
			return jwkKey{}, errors.New("invalid RSA exponent")
		}
		exponent := 0
		for _, value := range exponentBytes {
			exponent = exponent<<8 | int(value)
		}
		if exponent < 3 || exponent%2 == 0 {
			return jwkKey{}, errors.New("invalid RSA exponent")
		}
		publicKey := &rsa.PublicKey{N: new(big.Int).SetBytes(modulus), E: exponent}
		if publicKey.N.BitLen() < 2048 {
			return jwkKey{}, errors.New("RSA signing keys must be at least 2048 bits")
		}
		result.PublicKey = publicKey
	default:
		return jwkKey{}, errors.New("unsupported jwks key type")
	}
	return result, nil
}
