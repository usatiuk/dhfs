import {
    getAvailablePeers,
    getKnownPeers,
    getPeerAddresses,
    getSelfInfo,
    putKnownPeer,
    putPeerAddress,
    removeKnownPeer,
} from "./api/PeerState";
import { ActionFunctionArgs } from "react-router-dom";
import { hashCert } from "./hash";

export async function peerStateLoader() {
    const selfInfoApi = await getSelfInfo();
    const selfInfo = {
        ...selfInfoApi,
        certHash: await hashCert(selfInfoApi.cert),
    };
    const availablePeersApi = await getAvailablePeers();
    const availablePeers = await Promise.all(
        availablePeersApi.map(async (peerInfo) => {
            return {
                ...peerInfo,
                certHash: await hashCert(peerInfo.cert),
            };
        }),
    );
    const knownPeersApi = await getKnownPeers();
    const knownPeers = await Promise.all(
        knownPeersApi.map(async (peerInfo) => {
            return {
                ...peerInfo,
                certHash: await hashCert(peerInfo.cert),
            };
        }),
    );
    const peerAddresses = await getPeerAddresses();
    return {
        selfInfo,
        availablePeers,
        knownPeers,
        peerAddresses,
    };
}

export type PeerStateActionType =
    | "add_peer"
    | "remove_peer"
    | "save_addr"
    | unknown;

export async function peerStateAction({ request }: ActionFunctionArgs) {
    const formData = await request.formData();
    const intent = formData.get("intent") as PeerStateActionType;
    if (intent === "add_peer") {
        return await putKnownPeer(
            formData.get("uuid") as string,
            formData.get("cert") as string,
        );
    } else if (intent === "remove_peer") {
        return await removeKnownPeer(formData.get("uuid") as string);
    } else if (intent === "save_addr") {
        return await putPeerAddress(
            formData.get("uuid") as string,
            formData.get("address") as string,
        );
    } else {
        throw new Error("Malformed action: " + JSON.stringify(request));
    }
}
