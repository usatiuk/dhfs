import { TKnownPeerInfoTo } from "./api/dto";

import "./PeerKnownCard.scss";
import { useFetcher, useLoaderData } from "react-router-dom";
import { LoaderToType } from "./commonPlumbing";
import { peerStateLoader } from "./PeerStatePlumbing";

interface TKnownPeerInfoToWithHash extends TKnownPeerInfoTo {
    certHash: string;
}

export interface TPeerKnownCardProps {
    peerInfo: TKnownPeerInfoToWithHash;
}

export function PeerKnownCard({ peerInfo }: TPeerKnownCardProps) {
    const fetcher = useFetcher();
    const loaderData = useLoaderData() as LoaderToType<typeof peerStateLoader>;

    const addr = loaderData.peerAddresses.find(
        (item) => item.uuid === peerInfo.uuid,
    );

    return (
        <div className="peerKnownCard">
            <div className={"peerInfo"}>
                <div>
                    <span>UUID: </span>
                    <span>{peerInfo.uuid}</span>
                </div>
                <div>
                    <span>Certificate: {peerInfo.certHash}</span>
                </div>
                <div>
                    <fetcher.Form method="put" action={"/home/peers"}>
                        <span>Manual address: </span>
                        <input
                            name="intent"
                            hidden={true}
                            defaultValue={"save_addr"}
                        />
                        <input
                            name="uuid"
                            hidden={true}
                            defaultValue={peerInfo.uuid}
                        />
                        <input
                            name="address"
                            defaultValue={addr?.address || ""}
                            placeholder={"ip:port:secure port"}
                        />
                        <button type="submit">save</button>
                    </fetcher.Form>
                </div>
                <div>
                    <span>
                        {peerInfo.knownAddress ? "connected" : "not connected"}
                    </span>
                </div>
            </div>
            <fetcher.Form
                className="actions"
                method="put"
                action={"/home/peers"}
            >
                <button type="submit">remove</button>
                <input
                    name="intent"
                    hidden={true}
                    defaultValue={"remove_peer"}
                />
                <input name="uuid" hidden={true} defaultValue={peerInfo.uuid} />
            </fetcher.Form>
        </div>
    );
}
