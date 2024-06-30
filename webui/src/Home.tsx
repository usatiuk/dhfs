import { NavLink, Outlet } from "react-router-dom";

import "./Home.scss";

export function Home() {
    const activePendingClassName = ({
        isActive,
        isPending,
    }: {
        isActive: boolean;
        isPending: boolean;
    }) => (isActive ? "active" : isPending ? "pending" : "");

    return (
        <div id="Home">
            <div id="HomeSidebar">
                <div id="SidebarUserInfo">DHFS</div>
                <div id="SidebarNav">
                    <NavLink to={"peers"} className={activePendingClassName}>
                        Peers
                    </NavLink>{" "}
                </div>
            </div>
            <div id="HomeContent">
                <Outlet />
            </div>
        </div>
    );
}
