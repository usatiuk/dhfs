export type LoaderToType<T extends (...args: any) => any> =
    | Exclude<Awaited<ReturnType<T>>, Response>
    | undefined;
