package routify

import "testing"

func TestCalculateCost_Basic(t *testing.T) {
	c := CalculateCost(1_000_000, 500_000, PricingTier{
		ModelRate:      1.0, // $1 per 1M
		CompletionRate: 2.0,
		GroupRate:      1.0,
	})
	wantPrompt := 1.0
	wantCompletion := 1.0
	if abs(c.PromptCost-wantPrompt) > 1e-6 {
		t.Errorf("prompt cost: want %.6f got %.6f", wantPrompt, c.PromptCost)
	}
	if abs(c.CompletionCost-wantCompletion) > 1e-6 {
		t.Errorf("completion cost: want %.6f got %.6f", wantCompletion, c.CompletionCost)
	}
	if abs(c.TotalCost-2.0) > 1e-6 {
		t.Errorf("total: want 2.0 got %.6f", c.TotalCost)
	}
}

func TestCalculateCost_GroupRateApplied(t *testing.T) {
	cFree := CalculateCost(1_000_000, 0, PricingTier{ModelRate: 1, CompletionRate: 1, GroupRate: 0})
	cVIP := CalculateCost(1_000_000, 0, PricingTier{ModelRate: 1, CompletionRate: 1, GroupRate: 1.5})

	if cFree.TotalCost != 0 {
		t.Errorf("free user should have zero cost, got %.6f", cFree.TotalCost)
	}
	if cVIP.TotalCost != 1.5 {
		t.Errorf("vip group rate 1.5 should yield $1.50, got %.6f", cVIP.TotalCost)
	}
}

func TestCalculateCost_ZeroTokens(t *testing.T) {
	c := CalculateCost(0, 0, PricingTier{ModelRate: 1, CompletionRate: 2, GroupRate: 1.5})
	if c.TotalCost != 0 {
		t.Errorf("zero tokens should give zero cost, got %.6f", c.TotalCost)
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
