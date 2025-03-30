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
            <fetcher.Form
                className="actions"
                method="put"
                action={"/home/peers"}
            >
                <button type="submit">connect</button>
                <input name="intent" hidden={true} defaultValue={"add_peer"} />
                <input name="uuid" hidden={true} defaultValue={peerInfo.uuid} />
            </fetcher.Form>
        </div>
    );
}
