import React from "react";
import {
    createBrowserRouter,
    redirect,
    RouterProvider,
} from "react-router-dom";

import "./App.scss";
import { Home } from "./Home";
import { PeerState } from "./PeerState";
import { peerStateAction, peerStateLoader } from "./PeerStatePlumbing";
import { ErrorGate } from "./ErrorGate";

const router = createBrowserRouter(
    [
        {
            path: "/",
            loader: async () => {
                return redirect("/home");
                // if (getToken() == null) {
                //     return redirect("/login");
                // } else {
                //     return redirect("/home");
                // }
            },
            errorElement: <ErrorGate />,
        },
        {
            path: "/home",
            element: <Home />,
            children: [
                {
                    path: "peers",
                    element: <PeerState />,
                    loader: peerStateLoader,
                    action: peerStateAction,
                },
            ],
            errorElement: <ErrorGate />,
        },
    ],
    { basename: "/webui" },
);

export function App() {
    return (
        <React.StrictMode>
            <div id={"appRoot"}>
                <RouterProvider router={router} />
            </div>
        </React.StrictMode>
    );
}
