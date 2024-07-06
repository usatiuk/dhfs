import { getAvailablePeers } from "./api/PeerState";

export async function peerStateLoader() {
    return {
        availablePeers: await getAvailablePeers(),
    };
}
