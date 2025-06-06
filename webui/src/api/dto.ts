import { z } from "zod";

export const ErrorTo = z.object({
    errors: z.array(z.string()),
    code: z.number(),
});
export type TErrorTo = z.infer<typeof ErrorTo>;

export function isError(value: unknown): value is TErrorTo {
    return ErrorTo.safeParse(value).success;
}

function CreateAPIResponse<T extends z.ZodTypeAny>(obj: T) {
    return z.union([ErrorTo, obj]);
}

export const NoContentTo = z.object({});
export type TNoContentTo = z.infer<typeof NoContentTo>;

export const NoContentToResp = CreateAPIResponse(NoContentTo);
export type TNoContentToResp = z.infer<typeof NoContentToResp>;

// TokenRequest
export const TokenRequestTo = z.object({
    username: z.string(),
    password: z.string(),
});
export type TTokenRequestTo = z.infer<typeof TokenRequestTo>;

// Token
export const TokenTo = z.object({
    token: z.string(),
});
export type TTokenTo = z.infer<typeof TokenTo>;

export const TokenToResp = CreateAPIResponse(TokenTo);
export type TTokenToResp = z.infer<typeof TokenToResp>;

// SelfInfo

export const SelfInfoTo = z.object({
    selfUuid: z.string(),
    cert: z.string(),
});
export type TSelfInfoTo = z.infer<typeof SelfInfoTo>;

export const SelfInfoToResp = CreateAPIResponse(SelfInfoTo);
export type TSelfInfoToResp = z.infer<typeof SelfInfoToResp>;

// PeerInfo

export const PeerInfoTo = z.object({
    uuid: z.string(),
    knownAddress: z.string().optional(),
    cert: z.string(),
});
export type TPeerInfoTo = z.infer<typeof PeerInfoTo>;

//  AvailablePeerInfo
export const AvailablePeerInfoTo = PeerInfoTo;
export type TAvailablePeerInfoTo = z.infer<typeof AvailablePeerInfoTo>;

export const AvailablePeerInfoArrTo = z.array(AvailablePeerInfoTo);
export type TAvailablePeerInfoArrTo = z.infer<typeof AvailablePeerInfoArrTo>;

export const AvailablePeerInfoToResp = CreateAPIResponse(
    AvailablePeerInfoArrTo,
);
export type TAvailablePeerInfoToResp = z.infer<typeof AvailablePeerInfoToResp>;

// KnownPeerInfo
export const KnownPeerInfoTo = PeerInfoTo;
export type TKnownPeerInfoTo = z.infer<typeof KnownPeerInfoTo>;

export const KnownPeerInfoArrTo = z.array(KnownPeerInfoTo);
export type TKnownPeerInfoArrTo = z.infer<typeof KnownPeerInfoArrTo>;

export const KnownPeersTo = KnownPeerInfoArrTo;
export type TKnownPeersTo = z.infer<typeof KnownPeersTo>;

export const KnownPeersToResp = CreateAPIResponse(KnownPeersTo);
export type TKnownPeersToResp = z.infer<typeof KnownPeersToResp>;

// PeerAddressInfo
export const PeerAddressInfoTo = z.object({
    uuid: z.string(),
    address: z.string(),
});
export type TPeerAddressInfoTo = z.infer<typeof PeerAddressInfoTo>;

export const PeerAddressInfoArrTo = z.array(PeerAddressInfoTo);
export type TPeerAddressInfoArrTo = z.infer<typeof PeerAddressInfoArrTo>;

export const PeerAddressInfoToResp = CreateAPIResponse(PeerAddressInfoArrTo);
export type TPeerAddressInfoToResp = z.infer<typeof PeerAddressInfoToResp>;

// KnownPeerPut
export const KnownPeerPutTo = z.object({ cert: z.string() });
export type TKnownPeerPutTo = z.infer<typeof KnownPeerPutTo>;

// KnownPeerDelete
export const KnownPeerDeleteTo = NoContentTo;
export type TKnownPeerDeleteTo = z.infer<typeof KnownPeerDeleteTo>;
