import "./PeerState.scss";
import { useLoaderData } from "react-router-dom";
import { LoaderToType } from "./commonPlumbing";
import { peerStateLoader } from "./PeerStatePlumbing";
import { PeerAvailableCard } from "./PeerAvailableCard";
import { PeerKnownCard } from "./PeerKnownCard";

export function PeerState() {
    const loaderData = useLoaderData() as LoaderToType<typeof peerStateLoader>;

    const knownPeers = loaderData.knownPeers
        ?.filter((p) => p.uuid !== loaderData.selfInfo.selfUuid)
        ?.map((p) => <PeerKnownCard peerInfo={p} key={p.uuid} />);

    const availablePeers = loaderData.availablePeers.map((p) => (
        <PeerAvailableCard peerInfo={p} key={p.uuid} />
    ));

    return (
        <div id={"PeerState"}>
            <div className={"selfInfo"}>
                <div>This node:</div>
                <div>UUID: {loaderData.selfInfo.selfUuid}</div>
                <div>Certificate: {loaderData.selfInfo.certHash} </div>
            </div>
            <div>
                <div>Known peers</div>
                <div>{knownPeers}</div>
            </div>
            <div>
                <div>Available peers</div>
                <div>{availablePeers}</div>
            </div>
        </div>
    );
}
