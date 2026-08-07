import { createHash } from "node:crypto";
import type { TransitionReceipt } from "@swarm/protocol";

// Wave-0 receipt-signer INTERFACE + a deterministic in-memory fixture. Real
// cryptographic signing is deferred to the first storage-backed wave (per the
// frozen open-item resolution). This exists so Wave-1 receipt consumers can be
// written against a stable interface now.
//
// The fixture produces a deterministic SIGNATURE REFERENCE (not a real
// signature): a neutral `sigref:<hex>` string derived from the receipt's stable
// id fields. It carries no key material and no body content. Same receipt ->
// same signature ref (deterministic); different receipts -> different refs.

/** An opaque, neutral reference to a signature over a receipt. */
export type SignatureRef = string;

export type SignedReceipt = {
  readonly receiptId: TransitionReceipt["receiptId"];
  readonly signatureRef: SignatureRef;
};

/** The signer contract Wave-1 consumers depend on. */
export type ReceiptSigner = {
  /** Produce a signature reference for a receipt. */
  sign(receipt: TransitionReceipt): SignedReceipt;
  /** Verify that a signature reference matches a receipt. */
  verify(receipt: TransitionReceipt, signatureRef: SignatureRef): boolean;
};

/**
 * Deterministic in-memory fixture signer. The signature ref is a hash over the
 * receipt's identity fields only (receiptId + producerFactId + kind +
 * occurredAt), so it is stable and reproducible without any key material. This
 * is NOT cryptographically secure; it is a Wave-0 stand-in for interface tests.
 */
export function createDeterministicReceiptSigner(): ReceiptSigner {
  const digest = (receipt: TransitionReceipt): SignatureRef => {
    const identity = [
      receipt.receiptId,
      receipt.producerFactId,
      receipt.kind,
      receipt.occurredAt,
    ].join("|");
    const hex = createHash("sha256").update(`sigref:${identity}`).digest("hex");
    return `sigref:${hex}`;
  };

  return {
    sign(receipt: TransitionReceipt): SignedReceipt {
      return { receiptId: receipt.receiptId, signatureRef: digest(receipt) };
    },
    verify(receipt: TransitionReceipt, signatureRef: SignatureRef): boolean {
      return digest(receipt) === signatureRef;
    },
  };
}
