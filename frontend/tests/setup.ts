// Vitest test setup — happy-dom 15 ships a localStorage stub with no
// methods. We force-replace it with a minimal in-memory implementation
// so the theme persistence tests run.

class MemoryStorage implements Storage {
  private store = new Map<string, string>();
  get length(): number {
    return this.store.size;
  }
  key(index: number): string | null {
    return Array.from(this.store.keys())[index] ?? null;
  }
  getItem(key: string): string | null {
    return this.store.has(key) ? this.store.get(key)! : null;
  }
  setItem(key: string, value: string): void {
    this.store.set(key, String(value));
  }
  removeItem(key: string): void {
    this.store.delete(key);
  }
  clear(): void {
    this.store.clear();
  }
}

const ensureStorage = (name: "localStorage" | "sessionStorage"): void => {
  const existing = (globalThis as unknown as Record<string, unknown>)[name];
  const has =
    existing &&
    typeof (existing as Storage).getItem === "function" &&
    typeof (existing as Storage).setItem === "function";
  if (has) return;
  Object.defineProperty(globalThis, name, {
    value: new MemoryStorage(),
    writable: true,
    configurable: true,
  });
};

ensureStorage("localStorage");
ensureStorage("sessionStorage");
