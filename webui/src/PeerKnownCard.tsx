import { TKnownPeerInfoTo } from "./api/dto";

import "./PeerKnownCard.scss";

export interface TPeerKnownCardProps {
    peerInfo: TKnownPeerInfoTo;
}

export function PeerKnownCard({ peerInfo }: TPeerKnownCardProps) {
    return <div className="peerKnownCard">{peerInfo.uuid}</div>;
}
