/**
 * Defaults for real question–response SFT (no correctness/runtime metadata).
 */
export const FILTER_DEFAULTS: Record<string, Record<string, unknown>> = {
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
  signature_extraction: {},
};

export const FILTER_GROUPS: Record<string, string[]> = {
  core: [
    'remove_hacking',
    'remove_duplicates',
    'format_validity',
    'length_anomaly',
  ],
  analysis: ['signature_extraction'],
};

export const PRESETS: Record<string, { label: string; keys: string[] }> = {
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
