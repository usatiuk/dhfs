import { fetchJSON, fetchJSON_throws } from "./utils";
import {
    AvailablePeerInfoToResp,
    KnownPeerInfoToResp,
    NoContentToResp,
    PeerAddressInfoToResp,
    TAvailablePeerInfoArrTo,
    TAvailablePeerInfoToResp,
    TKnownPeerInfoArrTo,
    TKnownPeerInfoToResp,
    TNoContentToResp,
    TPeerAddressInfoArrTo,
    TPeerAddressInfoToResp,
} from "./dto";

export async function getAvailablePeers(): Promise<TAvailablePeerInfoArrTo> {
    return fetchJSON_throws<
        TAvailablePeerInfoToResp,
        typeof AvailablePeerInfoToResp
    >("/peers-manage/available-peers", "GET", AvailablePeerInfoToResp);
}

export async function getKnownPeers(): Promise<TKnownPeerInfoArrTo> {
    return fetchJSON_throws<TKnownPeerInfoToResp, typeof KnownPeerInfoToResp>(
        "/peers-manage/known-peers",
        "GET",
        KnownPeerInfoToResp,
    );
}

export async function putKnownPeer(uuid: string): Promise<TNoContentToResp> {
    return fetchJSON("/peers-manage/known-peers", "PUT", NoContentToResp, {
        uuid,
    });
}

export async function removeKnownPeer(uuid: string): Promise<TNoContentToResp> {
    return fetchJSON("/peers-manage/known-peers", "DELETE", NoContentToResp, {
        uuid,
    });
}

export async function getPeerAddresses(): Promise<TPeerAddressInfoArrTo> {
    return fetchJSON_throws<
        TPeerAddressInfoToResp,
        typeof PeerAddressInfoToResp
    >("/peers-addr-manage", "GET", PeerAddressInfoToResp);
}

export async function putPeerAddress(
    uuid: string,
    address: string,
): Promise<TNoContentToResp> {
    return fetchJSON(
        `/peers-addr-manage/${uuid}`,
        "PUT",
        NoContentToResp,
        address,
    );
}

export async function removePeerAddress(
    uuid: string,
): Promise<TNoContentToResp> {
    return fetchJSON(`/peers-addr-manage/${uuid}`, "DELETE", NoContentToResp);
}

export async function getPeerAddress(
    uuid: string,
): Promise<TPeerAddressInfoToResp> {
    return fetchJSON_throws<
        TPeerAddressInfoToResp,
        typeof PeerAddressInfoToResp
    >(`/peers-addr-manage/${uuid}`, "GET", PeerAddressInfoToResp);
}
