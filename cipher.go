package embeddb

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
)

var ErrCannotDecrypt = errors.New("cannot decrypt field")

// fieldCipher provides AES-256-GCM encryption for individual field values.
type fieldCipher struct {
	aead cipher.AEAD
}

func newFieldCipher(key []byte) (*fieldCipher, error) {
	if len(key) != 16 && len(key) != 24 && len(key) != 32 {
		return nil, fmt.Errorf("encryption key must be 16, 24, or 32 bytes (got %d)", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}
	return &fieldCipher{aead: aead}, nil
}

// encrypt appends the encrypted value (nonce + ciphertext + tag) to out.
func (fc *fieldCipher) encrypt(out, plaintext []byte) ([]byte, error) {
	if fc == nil {
		return nil, errors.New("no encryption key configured")
	}
	nonce := make([]byte, fc.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}
	out = append(out, nonce...)
	out = fc.aead.Seal(out, nonce, plaintext, nil)
	return out, nil
}

// decrypt extracts the nonce from data and decrypts the remainder.
func (fc *fieldCipher) decrypt(data []byte) ([]byte, error) {
	if fc == nil {
		return nil, errors.New("no encryption key configured")
	}
	if len(data) < fc.aead.NonceSize() {
		return nil, fmt.Errorf("%w: data too short for nonce", ErrCannotDecrypt)
	}
	nonce := data[:fc.aead.NonceSize()]
	ciphertext := data[fc.aead.NonceSize():]
	plaintext, err := fc.aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCannotDecrypt, err)
	}
	return plaintext, nil
}
