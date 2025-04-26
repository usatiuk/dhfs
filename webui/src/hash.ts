import * as forge from "node-forge";

export async function hashCert(cert: string) {
    const md = forge.md.sha1.create();
    md.update(cert);
    return md.digest().toHex();
}
