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

export const TokenRequestTo = z.object({
    username: z.string(),
    password: z.string(),
});
export type TTokenRequestTo = z.infer<typeof TokenRequestTo>;

export const TokenTo = z.object({
    token: z.string(),
});
export type TTokenTo = z.infer<typeof TokenTo>;

export const TokenToResp = CreateAPIResponse(TokenTo);
export type TTokenToResp = z.infer<typeof TokenToResp>;

export const AvailablePeerInfoTo = z.object({
    uuid: z.string(),
    addr: z.string(),
    port: z.number(),
});
export type TAvailablePeerInfoTo = z.infer<typeof AvailablePeerInfoTo>;

export const AvailablePeerInfoArrTo = z.array(AvailablePeerInfoTo);
export type TAvailablePeerInfoArrTo = z.infer<typeof AvailablePeerInfoArrTo>;

export const AvailablePeerInfoToResp = CreateAPIResponse(
    AvailablePeerInfoArrTo,
);
export type TAvailablePeerInfoToResp = z.infer<typeof AvailablePeerInfoToResp>;
