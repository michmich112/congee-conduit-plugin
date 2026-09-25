import { defaultKindsForRoles } from './lib/eventView.js';

export function defaultSettings() {
	return {
		index_backend: 'turso',
		postgres_url: '',
		postgres_user: '',
		postgres_password: '',
		product_kinds: defaultKindsForRoles(['product', 'listing']),
		stall_kinds: defaultKindsForRoles(['stall']),
		draft_kinds: defaultKindsForRoles(['listing_draft']),
		deletion_kinds: defaultKindsForRoles(['deletion']),
		index_drafts: false,
		geo_enabled: true,
		vector_enabled: true,
		embed_provider: 'on_device',
		embed_http_url: '',
		embed_http_model: '',
		embed_http_api_key: '',
		embed_dim: 384,
		embed_model_url: '',
		embed_runtime_url: '',
		active_filter: true,
		rank_all_product_reqs: false,
		inject_product_kinds_on_search: false,
		max_results: 0,
		geo_min_prefix_len: 2,
		search_candidate_cap: 1000,
		nip85_provider_pubkey: '78ed0837eba0ba244384195ce41d2a21575476a8e99e43f02d6e9729860e29e6',
		nip85_max_age_days: 14
	};
}

export function mergeSettings(raw) {
	const base = defaultSettings();
	if (!raw || typeof raw !== 'object') return base;
	return {
		...base,
		...raw,
		product_kinds: Array.isArray(raw.product_kinds) ? raw.product_kinds : base.product_kinds,
		stall_kinds: Array.isArray(raw.stall_kinds) ? raw.stall_kinds : base.stall_kinds,
		draft_kinds: Array.isArray(raw.draft_kinds) ? raw.draft_kinds : base.draft_kinds,
		deletion_kinds: Array.isArray(raw.deletion_kinds) ? raw.deletion_kinds : base.deletion_kinds,
		inject_product_kinds_on_search: false
	};
}

export function cloneSettings(s) {
	return JSON.parse(JSON.stringify(s));
}

export function settingsEqual(a, b) {
	return JSON.stringify(a) === JSON.stringify(b);
}
