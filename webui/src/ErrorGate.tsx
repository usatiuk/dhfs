import { useRouteError } from "react-router-dom";

export function ErrorGate() {
    const error = useRouteError();
    console.error(error);
    return <div>{JSON.stringify(error)}</div>;
}
