export class Logger {
  private static _enabled = process.env.NODE_ENV === "development";

  static trace(table: string, action: string, details: string) {
    if (!this._enabled) return;
    const time = new Date().toISOString().slice(11, 19);
    console.log(
      `\x1b[36m[${time}]\x1b[0m \x1b[33m[${table}]\x1b[0m ${action} → ${details}`,
    );
  }

  static warn(table: string, action: string, details: string) {
    if (!this._enabled) return;
    const time = new Date().toISOString().slice(11, 19);
    console.warn(
      `\x1b[36m[${time}]\x1b[0m \x1b[31m[${table}] ⚠️ ${action}\x1b[0m → ${details}`,
    );
  }
}
