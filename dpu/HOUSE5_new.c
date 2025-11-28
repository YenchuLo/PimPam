#include <dpu_mine.h>

/* --------------------------
 * Tunable parameters
 * -------------------------- */
#ifndef HEAVY_T2
#define HEAVY_T2              2048   /* mark (root ∩ second_root) ≥ this as heavy */
#endif

#ifndef MAX_HEAVY_PER_TSK
#define MAX_HEAVY_PER_TSK     64     /* max heavy items to cache per tasklet */
#endif

/* MRAM-aware size-only intersect */
static inline node_t intersect_seq_size_only(const __mram_ptr node_t *a, node_t na,
                                             const __mram_ptr node_t *b, node_t nb) {
    node_t ia = 0, ib = 0, cnt = 0;
    while (ia < na && ib < nb) {
        node_t va = a[ia], vb = b[ib];
        if (va == vb) { cnt++; ia++; ib++; }
        else if (va < vb) ia++;
        else ib++;
    }
    return cnt;
}

/* ----------------------------------------
 * Depth-2 worker (unchanged core logic)
 *  - if start=tasklet_id, step=NR_TASKLETS → cooperative mode
 *  - if start=0, step=1 → single-tasklet mode
 * ---------------------------------------- */
static ans_t __imp_house5_2(sysname_t tasklet_id,
                            node_t root, node_t second_root,
                            node_t start, node_t step) {
    node_t(*tasklet_buf)[BUF_SIZE] = buf[tasklet_id];

    edge_ptr root_begin         = row_ptr[root];
    edge_ptr root_end           = row_ptr[root + 1];
    edge_ptr second_root_begin  = row_ptr[second_root];
    edge_ptr second_root_end    = row_ptr[second_root + 1];

    ans_t ans = 0;

    /* fifth set = (second_root ∩ root) */
    node_t fifth_root_size =
        intersect_seq_buf_thresh(tasklet_buf,
            &col_idx[second_root_begin], second_root_end - second_root_begin,
            &col_idx[root_begin],        root_end - root_begin,
            mram_buf[tasklet_id + NR_TASKLETS], INVALID_NODE);

    node_t cur_cmp = 0;

    if (fifth_root_size) {
        for (edge_ptr j = root_begin + start; j < root_end; j += step) {
            node_t third_root = col_idx[j];
            if (third_root == second_root) continue;

            edge_ptr third_root_begin = row_ptr[third_root];
            edge_ptr third_root_end   = row_ptr[third_root + 1];

            /* fourth set = (second_root ∩ third_root) */
            node_t fourth_root_size =
                intersect_seq_buf_thresh(tasklet_buf,
                    &col_idx[second_root_begin], second_root_end - second_root_begin,
                    &col_idx[third_root_begin],  third_root_end  - third_root_begin,
                    mram_buf[tasklet_id], INVALID_NODE);

            if (!fourth_root_size) continue;

            /* common among (fourth, fifth) */
            node_t common_size =
                intersect_seq_buf_thresh(tasklet_buf,
                    mram_buf[tasklet_id],                 fourth_root_size,
                    mram_buf[tasklet_id + NR_TASKLETS],   fifth_root_size,
                    mram_buf[tasklet_id + (NR_TASKLETS << 1)], INVALID_NODE);

            /* adjust cur_fifth by skipping third_root if present in fifth */
            node_t cur_fifth = fifth_root_size;
            while (cur_cmp < fifth_root_size) {
                node_t fifth_root = mram_buf[tasklet_id + NR_TASKLETS][cur_cmp];
                if (fifth_root > third_root) break;
                else if (fifth_root == third_root) {
                    cur_fifth--;
                    cur_cmp++;
                    break;
                }
                cur_cmp++;
            }

            ans += ((ans_t)cur_fifth) * (fourth_root_size - 1) - common_size;
        }
    }
    return ans;
}

/* ----------------------------------------
 * Original single-tasklet fallback for a root
 * ---------------------------------------- */
static ans_t __imp_house5(sysname_t tasklet_id, node_t root) {
    edge_ptr root_begin = row_ptr[root];
    edge_ptr root_end   = row_ptr[root + 1];
    ans_t ans = 0;
    for (edge_ptr i = root_begin; i < root_end; i++) {
        node_t second_root = col_idx[i];
        if (second_root >= root) break;
        ans += __imp_house5_2(tasklet_id, root, second_root, 0, 1);
    }
    return ans;
}

/* ----------------------------------------
 * Adaptive depth-2 collaboration
 *  - Phase A: detect heavy second_root per tasklet (stride on j)
 *  - Phase B: token-passing to cooperatively process heavy ones
 *  - Phase C: owner tasklets process the remaining light ones
 * ---------------------------------------- */
static edge_ptr heavy_local_idx[NR_TASKLETS][MAX_HEAVY_PER_TSK];
static node_t   heavy_local_cnt[NR_TASKLETS];

extern void house5(sysname_t tasklet_id) {
    static ans_t partial_ans[NR_TASKLETS];
    static uint64_t partial_cycle[NR_TASKLETS];
    static perfcounter_cycles cycles[NR_TASKLETS];

    node_t i = 0;
    while (i < root_num) {
        node_t root       = roots[i];
        node_t root_begin = row_ptr[root];
        node_t root_end   = row_ptr[root + 1];

        /* small-degree roots → use the simple per-tasklet root distribution below */
        if (root_end - root_begin < BRANCH_LEVEL_THRESHOLD) {
            break;
        }

        barrier_wait(&co_barrier);
#ifdef PERF
        timer_start(&cycles[tasklet_id]);
#endif

        partial_ans[tasklet_id]      = 0;
        heavy_local_cnt[tasklet_id]  = 0;

        /* ---------- Phase A: detect heavy (stride by tasklet) ---------- */
        for (edge_ptr j = root_begin + tasklet_id; j < root_end; j += NR_TASKLETS) {
            node_t second_root = col_idx[j];
            if (second_root >= root) break;

            edge_ptr sb = row_ptr[second_root];
            edge_ptr se = row_ptr[second_root + 1];

            /* use |root ∩ second_root| as heaviness signal */
            node_t size_rs =
                intersect_seq_size_only(&col_idx[root_begin], root_end - root_begin,
                                        &col_idx[sb],         se - sb);

            if (size_rs >= HEAVY_T2 && heavy_local_cnt[tasklet_id] < MAX_HEAVY_PER_TSK) {
                heavy_local_idx[tasklet_id][heavy_local_cnt[tasklet_id]++] = j; /* remember index j */
            }
        }
        barrier_wait(&co_barrier);

        /* ---------- Phase B: depth-2 collaboration on heavy ---------- */
        static node_t shared_owner_count;
        static node_t shared_second_root;

        for (uint32_t owner = 0; owner < NR_TASKLETS; ++owner) {
            if (tasklet_id == owner) {
                shared_owner_count = heavy_local_cnt[owner];
            }
            barrier_wait(&co_barrier);

            node_t count = shared_owner_count;

            for (node_t k = 0; k < count; ++k) {
                if (tasklet_id == owner) {
                    edge_ptr j = heavy_local_idx[owner][k];
                    shared_second_root = col_idx[j];
                }
                barrier_wait(&co_barrier);

                /* cooperative depth-2: split by (start=tasklet_id, step=NR_TASKLETS) */
                ans_t add = __imp_house5_2(tasklet_id, root, shared_second_root,
                                           tasklet_id, NR_TASKLETS);

                if (tasklet_id == 0) partial_ans[0] += add;
                barrier_wait(&co_barrier);
            }
        }

        /* ---------- Phase C: light second_root handled independently ---------- */
        for (edge_ptr j = root_begin + tasklet_id; j < root_end; j += NR_TASKLETS) {
            node_t second_root = col_idx[j];
            if (second_root >= root) break;

            /* skip if this j was heavy for this tasklet */
            int is_heavy = 0;
            for (node_t k = 0; k < heavy_local_cnt[tasklet_id]; ++k) {
                if (heavy_local_idx[tasklet_id][k] == j) { is_heavy = 1; break; }
            }
            if (is_heavy) continue;

            /* single-tasklet processing for light items (less sync overhead) */
            partial_ans[tasklet_id] += __imp_house5_2(tasklet_id, root, second_root, 0, 1);
        }

#ifdef PERF
        partial_cycle[tasklet_id] = timer_stop(&cycles[tasklet_id]);
#endif
        barrier_wait(&co_barrier);

        if (tasklet_id == 0) {
            ans_t total_ans = 0;
#ifdef PERF
            uint64_t total_cycle = 0;
#endif
            for (uint32_t t = 0; t < NR_TASKLETS; ++t) {
                total_ans += partial_ans[t];
#ifdef PERF
                total_cycle += partial_cycle[t];
#endif
            }
            ans[i] = total_ans;          /* intended DMA */
#ifdef PERF
            cycle_ct[i] = total_cycle;   /* intended DMA */
#endif
        }
        i++;
    }

    /* Fallback: distribute remaining roots round-robin (unchanged) */
    for (i += tasklet_id; i < root_num; i += NR_TASKLETS) {
        node_t root = roots[i];
#ifdef PERF
        timer_start(&cycles[tasklet_id]);
#endif
        ans[i] = __imp_house5(tasklet_id, root);  /* intended DMA */
#ifdef PERF
        cycle_ct[i] = timer_stop(&cycles[tasklet_id]);  /* intended DMA */
#endif
    }
}
