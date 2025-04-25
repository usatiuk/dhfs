import { fetchJSON, fetchJSON_throws } from "./utils";
import {
    AvailablePeerInfoToResp,
    KnownPeersToResp,
    NoContentToResp,
    PeerAddressInfoToResp,
    SelfInfoToResp,
    TAvailablePeerInfoArrTo,
    TAvailablePeerInfoToResp,
    TKnownPeersTo,
    TKnownPeersToResp,
    TNoContentToResp,
    TPeerAddressInfoArrTo,
    TPeerAddressInfoToResp,
    TSelfInfoTo,
    TSelfInfoToResp,
} from "./dto";

export async function getSelfInfo(): Promise<TSelfInfoTo> {
    return fetchJSON_throws<TSelfInfoToResp, typeof SelfInfoToResp>(
        "/peer-info/self",
        "GET",
        SelfInfoToResp,
    );
}

export async function getAvailablePeers(): Promise<TAvailablePeerInfoArrTo> {
    return fetchJSON_throws<
        TAvailablePeerInfoToResp,
        typeof AvailablePeerInfoToResp
    >("/peers-manage/available-peers", "GET", AvailablePeerInfoToResp);
}

export async function getKnownPeers(): Promise<TKnownPeersTo> {
    return fetchJSON_throws<TKnownPeersToResp, typeof KnownPeersToResp>(
        "/peers-manage/known-peers",
        "GET",
        KnownPeersToResp,
    );
}

export async function putKnownPeer(
    uuid: string,
    cert: string,
): Promise<TNoContentToResp> {
    return fetchJSON(
        `/peers-manage/known-peers/${uuid}`,
        "PUT",
        NoContentToResp,
        {
            cert,
        },
    );
}

export async function removeKnownPeer(uuid: string): Promise<TNoContentToResp> {
    return fetchJSON(
        `/peers-manage/known-peers/${uuid}`,
        "DELETE",
        NoContentToResp,
        {},
    );
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
