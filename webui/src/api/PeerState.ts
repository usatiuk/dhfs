import { fetchJSON_throws } from "./utils";
import {
    AvailablePeerInfoToResp,
    TAvailablePeerInfoArrTo,
    TAvailablePeerInfoToResp,
} from "./dto";

export async function getAvailablePeers(): Promise<TAvailablePeerInfoArrTo> {
    return fetchJSON_throws<
        TAvailablePeerInfoToResp,
        typeof AvailablePeerInfoToResp
    >("/objects-manage/available-peers", "GET", AvailablePeerInfoToResp);
}
