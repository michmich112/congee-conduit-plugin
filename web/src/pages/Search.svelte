<script>
	import InfoLabel from '../lib/InfoLabel.svelte';
	import Switch from '../lib/Switch.svelte';
	import Tooltip from '../lib/Tooltip.svelte';

	let { settings = $bindable() } = $props();
	let showAdvanced = $state(false);
</script>

<section class="space-y-6">
	<div>
		<div class="flex items-center gap-1.5">
			<h2 class="text-lg font-medium text-neutral-900 dark:text-neutral-100">Search</h2>
			<Tooltip
				label="About Search"
				tip="Conduit never rewrites the kinds on a client's REQ. When a REQ already asks for product kinds and includes a search string or #g, Conduit answers with ranked listing event IDs from its index. A search with no kinds is left to the relay."
			/>
		</div>
		<p class="mt-1 text-sm text-neutral-500 dark:text-neutral-400">
			When Conduit intercepts a product-kind REQ, it keeps the client's filter kinds and returns
			ranked listing IDs from the index.
		</p>
	</div>

	<div class="space-y-3">
		<Switch
			bind:checked={settings.rank_all_product_reqs}
			label="Rank all product REQs"
			description="If a REQ already asks for product kinds but has no search string (for example kinds:[30402]), still return Conduit's ranked listing IDs instead of the relay's newest-first query."
			tip="Off (default): Conduit only ranks when the REQ has a search string or a #g geohash. On: even a bare product-kind REQ such as kinds 30402 is answered from the index order instead of the relay's created_at sort. Other kinds still pass through."
		/>
	</div>

	<div class="grid gap-3 sm:grid-cols-2">
		<label class="block space-y-1">
			<InfoLabel
				text="Max results"
				tip="Plugin ceiling on how many event IDs Conduit returns on intercept. 0 means no extra cap: honor the client's REQ limit, or 500 if the client omitted one. This is not a global marketplace size limit."
			/>
			<input
				class="w-full rounded-lg border border-neutral-200 bg-white px-3 py-2 text-sm text-neutral-900 outline-none focus:ring-2 focus:ring-neutral-400 dark:border-neutral-800 dark:bg-neutral-900 dark:text-neutral-100"
				type="number"
				min="0"
				bind:value={settings.max_results}
			/>
			<span class="text-xs text-neutral-500 dark:text-neutral-400">
				0 = no plugin cap (use the REQ limit, else 500).
			</span>
		</label>
		<label class="block space-y-1">
			<InfoLabel
				text="Geo min prefix"
				tip="How many characters of a #g geohash are used for the SQL prefix match. Example: 2 turns 9q8yy into 9q%, which is a wide area. Matching rows are then sorted by distance from the full geohash. Shorter prefixes return more candidates."
			/>
			<input
				class="w-full rounded-lg border border-neutral-200 bg-white px-3 py-2 text-sm text-neutral-900 outline-none focus:ring-2 focus:ring-neutral-400 dark:border-neutral-800 dark:bg-neutral-900 dark:text-neutral-100"
				type="number"
				min="1"
				max="12"
				bind:value={settings.geo_min_prefix_len}
			/>
			<span class="text-xs text-neutral-500 dark:text-neutral-400">
				Characters of #g used for prefix match; then sort by distance.
			</span>
		</label>
	</div>

	<div class="space-y-3 rounded-lg border border-neutral-200 p-4 dark:border-neutral-800">
		<div>
			<h3 class="text-sm font-medium text-neutral-900 dark:text-neutral-100">Merchant trust signal</h3>
			<p class="mt-1 text-xs text-neutral-500 dark:text-neutral-400">
				A current NIP-85 rank can modestly improve product ordering. Merchants without one remain searchable. Congee must sync the provider's signed assertions separately.
			</p>
		</div>
		<label class="block space-y-1">
			<InfoLabel text="NIP-85 provider pubkey" tip="The signer of kind 30382 rank assertions. Clear this field to disable the trust signal. The default is the published Brainstorm perspective selected for Conduit." />
			<input
				class="w-full rounded-lg border border-neutral-200 bg-white px-3 py-2 font-mono text-xs text-neutral-900 outline-none focus:ring-2 focus:ring-neutral-400 dark:border-neutral-800 dark:bg-neutral-900 dark:text-neutral-100"
				type="text"
				spellcheck="false"
				bind:value={settings.nip85_provider_pubkey}
			/>
		</label>
		<label class="block space-y-1 sm:max-w-xs">
			<InfoLabel text="Maximum assertion age (days)" tip="Older assertions are ignored in search. This is based on the signed event timestamp." />
			<input
				class="w-full rounded-lg border border-neutral-200 bg-white px-3 py-2 text-sm text-neutral-900 outline-none focus:ring-2 focus:ring-neutral-400 dark:border-neutral-800 dark:bg-neutral-900 dark:text-neutral-100"
				type="number"
				min="1"
				bind:value={settings.nip85_max_age_days}
			/>
		</label>
	</div>

	<div>
		<button
			class="text-sm font-medium text-neutral-600 underline-offset-2 hover:underline dark:text-neutral-300"
			type="button"
			onclick={() => (showAdvanced = !showAdvanced)}
		>
			{showAdvanced ? 'Hide advanced' : 'Advanced'}
		</button>
		{#if showAdvanced}
			<label class="mt-3 block space-y-1">
				<InfoLabel
					text="Search candidate cap"
					tip="How many matching listings SQL may load before ranking. This is a performance bound, not the number of events sent to the client. After this prefetch, Conduit ranks (vector and/or geo) and then applies Max results / the REQ limit. Higher is more complete and more expensive."
				/>
				<input
					class="w-full rounded-lg border border-neutral-200 bg-white px-3 py-2 text-sm text-neutral-900 outline-none focus:ring-2 focus:ring-neutral-400 dark:border-neutral-800 dark:bg-neutral-900 dark:text-neutral-100"
					type="number"
					min="1"
					bind:value={settings.search_candidate_cap}
				/>
			</label>
		{/if}
	</div>
</section>
