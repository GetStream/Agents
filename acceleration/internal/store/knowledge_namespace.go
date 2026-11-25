package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// The accounts a knowledge base is kept in (KnowledgePlace.Account).
const (
	// KnowledgeAccountStream is the deployment's own account at the knowledge vendor.
	KnowledgeAccountStream = "stream"
	// KnowledgeAccountCustomer is the customer's own, on the key they keep for the base's region.
	KnowledgeAccountCustomer = "customer"
)

// KnowledgePlace is where one of a customer's knowledge bases is kept: a region, and an account
// in it.
type KnowledgePlace struct {
	Region  string
	Account string
}

// KnowledgeNamespace is where one of a customer's knowledge bases is kept.
type KnowledgeNamespace struct {
	bun.BaseModel `bun:"table:knowledge_namespaces"`

	CustomerID string    `bun:"customer_id,pk"`
	Namespace  string    `bun:"namespace,pk"`
	Region     string    `bun:"region,notnull"`
	Account    string    `bun:"account,notnull"`
	CreatedAt  time.Time `bun:"created_at,notnull"`
}

// Place is where the base is kept.
func (n KnowledgeNamespace) Place() KnowledgePlace {
	return KnowledgePlace{Region: n.Region, Account: n.Account}
}

// Validate refuses a namespace the table would refuse.
func (n *KnowledgeNamespace) Validate() error {
	return validate("knowledge namespace").
		required("customer_id", n.CustomerID).
		required("namespace", n.Namespace).
		check(regionPattern.MatchString(n.Region), "region", "is not a region such as us or eu").
		oneOf("account", n.Account, KnowledgeAccountStream, KnowledgeAccountCustomer).
		done()
}

// KnowledgeNamespaces are where customers' knowledge bases are kept, in the shared database.
type KnowledgeNamespaces struct {
	store *Store
}

// GetPlace is where a customer's knowledge base is kept, and false for one nothing has been
// written to through the router.
func (r *KnowledgeNamespaces) GetPlace(ctx context.Context, customerID, namespace string) (KnowledgePlace, bool, error) {
	var kept KnowledgeNamespace
	err := r.store.global.NewSelect().Model(&kept).
		Where("customer_id = ?", customerID).
		Where("namespace = ?", namespace).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return KnowledgePlace{}, false, nil
	}
	if err != nil {
		return KnowledgePlace{}, false, stack.Wrap(fmt.Errorf("store: knowledge place: %w", err))
	}
	return kept.Place(), true, nil
}

// Claim records that a customer's knowledge base is kept at place, unless it is kept somewhere
// already, and returns where it is kept: of two first writes at once, in two regions or
// accounts, the one that lands first is where the base stays.
func (r *KnowledgeNamespaces) Claim(ctx context.Context, customerID, namespace string, place KnowledgePlace) (KnowledgePlace, error) {
	if place.Account == "" {
		place.Account = KnowledgeAccountStream
	}
	kept := KnowledgeNamespace{CustomerID: customerID, Namespace: namespace, Region: place.Region, Account: place.Account,
		CreatedAt: time.Now().UTC()}
	if err := kept.Validate(); err != nil {
		return KnowledgePlace{}, stack.Wrap(err)
	}
	// A no-op update, so RETURNING answers the row that was already there.
	_, err := r.store.global.NewInsert().Model(&kept).
		On("CONFLICT (customer_id, namespace) DO UPDATE").
		Set("customer_id = EXCLUDED.customer_id").
		Returning("region, account").
		Exec(ctx)
	if err != nil {
		return KnowledgePlace{}, stack.Wrap(fmt.Errorf("store: claim knowledge place: %w", err))
	}
	return kept.Place(), nil
}

// Delete forgets where a customer's knowledge base is kept, once it is dropped.
func (r *KnowledgeNamespaces) Delete(ctx context.Context, customerID, namespace string) error {
	_, err := r.store.global.NewDelete().Model((*KnowledgeNamespace)(nil)).
		Where("customer_id = ?", customerID).
		Where("namespace = ?", namespace).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: forget knowledge place: %w", err))
	}
	return nil
}

// SelectInCustomerAccounts is every knowledge base kept in a customer's own account, by
// customer and then namespace: what the hourly storage job reads on each one's key, since the
// deployment's account does not list them.
func (r *KnowledgeNamespaces) SelectInCustomerAccounts(ctx context.Context) ([]KnowledgeNamespace, error) {
	kept := []KnowledgeNamespace{}
	err := r.store.global.NewSelect().Model(&kept).
		Where("account = ?", KnowledgeAccountCustomer).
		Order("customer_id", "namespace").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: knowledge in customer accounts: %w", err))
	}
	return kept, nil
}
