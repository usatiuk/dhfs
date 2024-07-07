import { TKnownPeerInfoTo } from "./api/dto";

import "./PeerKnownCard.scss";
import { useFetcher } from "react-router-dom";

export interface TPeerKnownCardProps {
    peerInfo: TKnownPeerInfoTo;
}

export function PeerKnownCard({ peerInfo }: TPeerKnownCardProps) {
    const fetcher = useFetcher();

    return (
        <div className="peerKnownCard">
            <div className={"peerInfo"}>
                <span>UUID: </span>
                <span>{peerInfo.uuid}</span>
            </div>
            <fetcher.Form
                className="actions"
                method="put"
                action={"/home/peers"}
            >
                <button type="submit">remove</button>
                <input name="intent" hidden={true} value={"remove_peer"} />
                <input name="uuid" hidden={true} value={peerInfo.uuid} />
            </fetcher.Form>
        </div>
    );
}
