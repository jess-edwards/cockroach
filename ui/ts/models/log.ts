// source: models/log.ts
/// <reference path="../models/proto.ts" />
/// <reference path="../typings/d3/d3.d.ts" />
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="../util/chainprop.ts" />
/// <reference path="../util/format.ts" />
/// <reference path="../util/querycache.ts" />
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

/**
 * Models contains data models pulled from cockroach.
 */
module Models {
  "use strict";
  /**
   * Log package represents the logs collected by each Cockroach node.
   */
  export module Log {
    import Promise = _mithril.MithrilPromise;
    import Property = _mithril.MithrilProperty;

    export interface LogResponseSet {
      d: Proto.LogEntry[];
    }

    export class Entries {
      public allEntries: Utils.ReadOnlyProperty<Proto.LogEntry[]>;

      startTime: Property<number> = m.prop(<number>null);
      endTime: Property<number> = m.prop(<number>null);
      max: Property<number> = m.prop(<number>null);
      level: Property<string> = m.prop(<string>null);
      pattern: Property<string> = m.prop(<string>null);
      node: Property<string> = m.prop(<string>null);

      refresh: () => void = () => {
        this._data.refresh();
      };

      result: () => Proto.LogEntry[] = () => {
        return this._data.result();
      };

      nodeName: () => string = () => {
        if ((this.node() != null) && (this.node() !== "local")) {
          return this.node();
        }
        return "Local";
      };

      private _data: Utils.QueryCache<Proto.LogEntry[]> = new Utils.QueryCache(
        (): Promise<Proto.LogEntry[]> => {
          return m.request({ url: this._url(), method: "GET", extract: nonJsonErrors })
            .then((results: LogResponseSet) => {
              return results.d;
            });
        },
        true
      );

      /**
       * buildURL creates the route and query parameters based on the current
       * state of all the properties on entries.
       * TODO(bram): consider exporting this to a utility function and maybe
       * adding an interface for these top level models. Tamir also suggested
       * moving to a single param with a tighter serialized format.
       */
      buildURL(): string {
        let url: string = "/logs/";
        if (this.node() != null) {
          url += encodeURIComponent(this.node());
        } else {
          url += "local";
        }
        let first = true;
        if (this.level()) {
          url += "?";
          first = false;
          url += "level=" + encodeURIComponent(this.level());
        }
        if (this.startTime()) {
          url += first ? "?" : "&";
          first = false;
          url += "startTime=" + encodeURIComponent(this.startTime().toString());
        }
        if (this.endTime()) {
          url += first ? "?" : "&";
          first = false;
          url += "endTime=" + encodeURIComponent(this.endTime().toString());
        }
        if (this.max()) {
          url += first ? "?" : "&";
          first = false;
          url += "max=" + encodeURIComponent(this.max().toString());
        }
        if ((this.pattern()) && (this.pattern().length > 0)) {
          url += first ? "?" : "&";
          first = false;
          url += "pattern=" + encodeURIComponent(this.pattern());
        }
        return url;
      }

      /**
       * _url return the url used for queries to the status server.
       */
      private _url(): string {
        return "/_status" + this.buildURL();
      }

      constructor() {
        this.level(Utils.Format.Severity(2));
        this.allEntries = this._data.result;
      }
    }

    /**
     * nonJsonErrors ensures that error messages returned from the server
     * are parseable as JSON strings.
     */
    function nonJsonErrors(xhr: XMLHttpRequest, opts: _mithril.MithrilXHROptions): string {
      return xhr.status > 200 ? JSON.stringify(xhr.responseText) : xhr.responseText;
    }
  }
}
