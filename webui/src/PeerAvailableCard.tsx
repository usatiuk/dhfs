import { TAvailablePeerInfoTo } from "./api/dto";
import { useFetcher } from "react-router-dom";

import "./PeerAvailableCard.scss";

interface TAvailablePeerInfoToWithHash extends TAvailablePeerInfoTo {
    certHash: string;
}

export interface TPeerAvailableCardProps {
    peerInfo: TAvailablePeerInfoToWithHash;
}

export function PeerAvailableCard({ peerInfo }: TPeerAvailableCardProps) {
    const fetcher = useFetcher();
    return (
        <div className="peerAvailableCard">
            <div className={"peerInfo"}>
                <div>
                    <span>UUID: </span>
                    <span>{peerInfo.uuid}</span>
                </div>
                <div>
                    <span>Cert: {peerInfo.certHash}</span>
                </div>
            </div>
            <fetcher.Form
                className="actions"
                method="put"
                action={"/home/peers"}
            >
                <button type="submit">connect</button>
                <input name="intent" hidden={true} defaultValue={"add_peer"} />
                <input name="uuid" hidden={true} defaultValue={peerInfo.uuid} />
                <input name="cert" hidden={true} defaultValue={peerInfo.cert} />
            </fetcher.Form>
        </div>
    );
}
