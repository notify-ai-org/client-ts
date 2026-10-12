export interface Logger {
  info(msg: string, ...args: unknown[]): void;
  warn(msg: string, ...args: unknown[]): void;
  error(msg: string, ...args: unknown[]): void;
}

export const consoleLogger: Logger = {
  info: (m, ...a) => console.info(`[notify] ${m}`, ...a),
  warn: (m, ...a) => console.warn(`[notify] ${m}`, ...a),
  error: (m, ...a) => console.error(`[notify] ${m}`, ...a),
};