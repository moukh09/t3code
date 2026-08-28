import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

/** Per-instance adapter shape for the direct GitHub Copilot CLI driver. */
export interface GitHubCopilotAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {}
