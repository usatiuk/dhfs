import {
    getAvailablePeers,
    getKnownPeers,
    putKnownPeer,
} from "./api/PeerState";
import { ActionFunctionArgs } from "react-router-dom";

export async function peerStateLoader() {
    return {
        availablePeers: await getAvailablePeers(),
        knownPeers: await getKnownPeers(),
    };
}

export type PeerStateActionType = "add_peer" | unknown;

export async function peerStateAction({ request }: ActionFunctionArgs) {
    const formData = await request.formData();
    const intent = formData.get("intent") as PeerStateActionType;
    if (intent === "add_peer") {
        return await putKnownPeer(formData.get("uuid") as string);
    } else {
        throw new Error("Malformed action: " + JSON.stringify(request));
    }
}
