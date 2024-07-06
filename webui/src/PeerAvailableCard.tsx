import { TAvailablePeerInfoTo } from "./api/dto";
import { useFetcher } from "react-router-dom";

import "./PeerAvailableCard.scss";

export interface TPeerAvailableCardProps {
    peerInfo: TAvailablePeerInfoTo;
}

export function PeerAvailableCard({ peerInfo }: TPeerAvailableCardProps) {
    const fetcher = useFetcher();

    return (
        <div className="peerAvailableCard">
            <div className={"peerInfo"}>
                <span>UUID: </span>
                <span>{peerInfo.uuid}</span>
            </div>
            <fetcher.Form method="put" action={"/home/peers"}>
                <input name="intent" hidden={true} value={"add_peer"} />
                <input name="uuid" hidden={true} value={peerInfo.uuid} />
                <button type="submit">connect</button>
            </fetcher.Form>
        </div>
    );
}
