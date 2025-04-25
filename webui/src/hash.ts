export async function hashCert(cert: string) {
    const hash = await crypto.subtle.digest(
        "SHA-1",
        new TextEncoder().encode(cert),
    );
    return btoa(String.fromCharCode(...new Uint8Array(hash)));
}
