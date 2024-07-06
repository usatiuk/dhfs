export type LoaderToType<T extends (...args: unknown[]) => unknown> = Awaited<
    ReturnType<T>
>;
