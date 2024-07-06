import { fetchJSON, fetchJSON_throws } from "./utils";
import {
    AvailablePeerInfoToResp,
    KnownPeerInfoToResp,
    NoContentToResp,
    TAvailablePeerInfoArrTo,
    TAvailablePeerInfoToResp,
    TKnownPeerInfoArrTo,
    TKnownPeerInfoToResp,
    TNoContentToResp,
} from "./dto";

export async function getAvailablePeers(): Promise<TAvailablePeerInfoArrTo> {
    return fetchJSON_throws<
        TAvailablePeerInfoToResp,
        typeof AvailablePeerInfoToResp
    >("/objects-manage/available-peers", "GET", AvailablePeerInfoToResp);
}

export async function getKnownPeers(): Promise<TKnownPeerInfoArrTo> {
    return fetchJSON_throws<TKnownPeerInfoToResp, typeof KnownPeerInfoToResp>(
        "/objects-manage/known-peers",
        "GET",
        KnownPeerInfoToResp,
    );
}

export async function putKnownPeer(uuid: string): Promise<TNoContentToResp> {
    return fetchJSON("/objects-manage/known-peers", "PUT", NoContentToResp, {
        uuid,
    });
}
