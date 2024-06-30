import { jwtDecode } from "jwt-decode";
import { isError } from "./dto";

declare const process: {
    env: {
        NODE_ENV: string;
    };
};

const apiRoot: string =
    process.env.NODE_ENV == "production" ? "" : "http://localhost:8080";

let token: string | null;

export function setToken(_token: string): void {
    token = _token;
    localStorage.setItem("jwt_token", token);
}

export function getToken(): string | null {
    if (!token && localStorage.getItem("jwt_token") != null) {
        token = localStorage.getItem("jwt_token");
    }
    return token;
}

export function getTokenUserUuid(): string | null {
    const token = getToken();
    if (!token) return null;
    return jwtDecode(token).sub ?? null;
}

export function deleteToken(): void {
    token = null;
    localStorage.removeItem("jwt_token");
}

export async function fetchJSON<T, P extends { parse: (arg: string) => T }>(
    path: string,
    method: string,
    parser: P,
    body?: string | Record<string, unknown> | File,
    headers?: Record<string, string>,
): Promise<T> {
    const reqBody = () =>
        body instanceof File
            ? (() => {
                  const fd = new FormData();
                  fd.append("file", body);
                  return fd;
              })()
            : JSON.stringify(body);

    const reqHeaders = () =>
        body instanceof File
            ? headers
            : { ...headers, "Content-Type": "application/json" };

    const response = await fetch(apiRoot + path, {
        method,
        headers: reqHeaders(),
        body: reqBody(),
    });

    const json = await response.json().catch(() => {
        return {};
    });
    const parsed = parser.parse(json);
    if (isError(parsed)) {
        alert(parsed.errors.join(", "));
    }
    return parsed;
}

export async function fetchJSONAuth<T, P extends { parse: (arg: string) => T }>(
    path: string,
    method: string,
    parser: P,
    body?: string | Record<string, unknown> | File,
    headers?: Record<string, unknown>,
): Promise<T> {
    if (token) {
        return fetchJSON(path, method, parser, body, {
            ...headers,
            Authorization: `Bearer ${token}`,
        });
    } else {
        throw new Error("Not logged in");
    }
}
