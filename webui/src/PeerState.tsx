import "./PeerState.scss";
import { useLoaderData } from "react-router-dom";
import { LoaderToType } from "./commonPlumbing";
import { peerStateLoader } from "./PeerStatePlumbing";

export function PeerState() {
    const loaderData = useLoaderData() as LoaderToType<typeof peerStateLoader>;

    const availablePeers = loaderData?.availablePeers.map((p) => {
        return <div>{p.uuid}</div>;
    });

    return <div id={"PeerState"}>{availablePeers}</div>;
}
