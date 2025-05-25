from cherry_core.svm_decode import InstructionSignature, ParamInput, DynType

_MEMO_PROGRAM_ID_V1 = "Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo"
_MEMO_PROGRAM_ID_V2 = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"

_TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
_TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
_TOKEN_TRANSFER_DISCRIMINATOR = bytes([3])
_TOKEN_TRANSFER_CHECKED_DISCRIMINATOR = bytes([12])
_TOKEN_TRANSFER_SIGNATURE = InstructionSignature(
    discriminator=_TOKEN_TRANSFER_DISCRIMINATOR,
    params=[
        ParamInput(
            name="amount",
            param_type=DynType.U64,
        ),
    ],
    accounts_names=[
        "source",
        "destination",
        "authority",
    ],
)
_TOKEN_TRANSFER_CHECKED_SIGNATURE = InstructionSignature(
    discriminator=_TOKEN_TRANSFER_CHECKED_DISCRIMINATOR,
    params=[
        ParamInput(
            name="amount",
            param_type=DynType.U64,
        ),
        ParamInput(
            name="decimals",
            param_type=DynType.U8,
        ),
    ],
    accounts_names=[
        "source",
        "mint",
        "destination",
        "authority",
    ],
)
