/**
 * Defaults for real question–response SFT (no correctness/runtime metadata).
 */
export const FILTER_DEFAULTS: Record<string, Record<string, unknown>> = {
  custom_script: {
    code: '',
    name: 'Custom script',
    removal_reason_label: 'custom_script',
  },
  remove_hacking: { level: 2, use_dataset_hacked_field: false },
  remove_duplicates: {
    mode: 'question+response',
    reasoning_repetition: true,
  },
  format_validity: {
    require_modelnew: true,
    require_load_inline: true,
    require_global_kernel: true,
    require_forward: true,
    require_cuda_source: false,
  },
  length_anomaly: {
    min_question_chars: 0,
    max_question_chars: 2000000,
    min_response_chars: 0,
    max_response_chars: 2000000,
    detect_truncation: true,
  },
  random_drop: { drop_fraction: 0.1, random_seed: 42 },
  balance_to_mean: { group_by: 'signature', random_seed: 42 },
};

export const FILTER_GROUPS: Record<string, string[]> = {
  cleanup: ['remove_hacking', 'remove_duplicates'],
  validity: ['format_validity', 'length_anomaly'],
  balancing: ['random_drop', 'balance_to_mean'],
  script: ['custom_script'],
};

/** Group legend tooltip only */
export const GROUP_HELP: Record<string, string> = {
  script:
    'User-defined Python on the API server: removal_mask(df, config) → bool Series (True = remove row). Requires ALLOW_CUSTOM_SCRIPT_FILTERS=1.',
  cleanup:
    'Clean the text: flag likely jailbreak / policy-evasion patterns, then remove duplicate question–answer pairs.',
  validity:
    'Validate shape: required SFT markers in the answer and reasonable length, including likely truncation.',
  balancing:
    'Optionally drop rows at random or downsample heavy signature or stage_focus buckets toward the mean.',
};

/** Short UI title per filter id */
export const FILTER_LABELS: Record<string, string> = {
  custom_script: 'Custom script (Python)',
  remove_hacking: 'Remove hacking',
  remove_duplicates: 'Remove duplicates',
  format_validity: 'Format validity',
  length_anomaly: 'Length / truncation',
  random_drop: 'Random drop',
  balance_to_mean: 'Balance to mean',
};

/** Tooltip only — explain what the filter does for curators */
export const FILTER_HELP: Record<string, string> = {
  custom_script:
    'Provide removal_mask(df, config) returning a pandas Series aligned with df.index; True marks rows to remove. Runs only when the server sets ALLOW_CUSTOM_SCRIPT_FILTERS=1.',
  remove_hacking:
    'Uses text heuristics to catch rows that look like jailbreaks, “ignore previous rules”, hidden instructions, or other attempts to manipulate the model; severity is controlled by the level setting.',
  remove_duplicates: 'Removes exact or near-duplicate question–answer pairs so the dataset is not padded with repeats.',
  format_validity:
    'Checks that each kept line still contains the SFT markers you require (for example MODEL/NEW, load-inline, global kernel blocks) so every row matches your expected training format.',
  length_anomaly: 'Flags rows where the question or answer is unusually short or long, or looks cut off mid-answer.',
  random_drop: 'Randomly removes a configurable fraction of rows (useful for quick downsampling; seed controls repeatability).',
  balance_to_mean:
    'Reduces oversized buckets (by signature or stage_focus title) toward the average count. Requires at least two different values for that column in the rows you run on (a subset that only contains one signature cannot be balanced across signatures).',
};

/** Preset buttons shown in this order */
export const PRESET_ORDER = ['basic', 'format', 'full'] as const;

export const PRESETS: Record<
  (typeof PRESET_ORDER)[number],
  { label: string; keys: string[] }
> = {
  basic: {
    label: 'Basic cleanup',
    keys: ['remove_hacking', 'remove_duplicates'],
  },
  format: {
    label: 'Format cleanup',
    keys: ['format_validity', 'length_anomaly'],
  },
  full: {
    label: 'Full cleanup',
    keys: [
      'remove_hacking',
      'remove_duplicates',
      'format_validity',
      'length_anomaly',
    ],
  },
};
