# inventory/exceptions.py
"""
Custom exceptions for the inventory accounting engine.

These exceptions are used to provide clear, actionable error messages
when inventory posting operations fail. Never silence these with bare
`except Exception: pass` — always log or re-raise.
"""


class InventoryPostingError(Exception):
    """
    Raised when an inventory posting operation fails.

    This exception captures full context about the failing document so
    that the source of the failure can be traced in logs and audit reports.
    """

    def __init__(self, message, *, source_type=None, source_id=None,
                 product=None, company=None):
        super().__init__(message)
        self.source_type = source_type
        self.source_id = source_id
        self.product = product
        self.company = company

    def __str__(self):
        base = super().__str__()
        ctx_parts = []
        if self.source_type:
            ctx_parts.append(f"source_type={self.source_type}")
        if self.source_id is not None:
            ctx_parts.append(f"source_id={self.source_id}")
        if self.product is not None:
            name = getattr(self.product, "name", None) or str(self.product)
            ctx_parts.append(f"product={name!r}")
        if self.company is not None:
            name = getattr(self.company, "name", None) or str(self.company)
            ctx_parts.append(f"company={name!r}")
        if ctx_parts:
            return f"{base} [{', '.join(ctx_parts)}]"
        return base
