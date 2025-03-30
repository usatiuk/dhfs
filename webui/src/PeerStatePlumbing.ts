import {
    getAvailablePeers,
    getKnownPeers,
    getPeerAddresses,
    putKnownPeer,
    putPeerAddress,
    removeKnownPeer,
} from "./api/PeerState";
import { ActionFunctionArgs } from "react-router-dom";

export async function peerStateLoader() {
    return {
        availablePeers: await getAvailablePeers(),
        knownPeers: await getKnownPeers(),
        peerAddresses: await getPeerAddresses(),
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
        return await putKnownPeer(formData.get("uuid") as string);
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
