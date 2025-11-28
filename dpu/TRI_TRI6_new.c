#include <dpu_mine.h>

/* =========================
 *  Tunable parameters
 * ========================= */
#ifndef HEAVY_T2
#define HEAVY_T2              2048   /* threshold: if |root ∩ second| >= this value, mark as heavy */
#endif

#ifndef MAX_HEAVY_PER_TSK
#define MAX_HEAVY_PER_TSK     64     /* max number of heavy second_root entries per tasklet */
#endif

/* =========================
 *  Forward declarations
 * ========================= */
static inline node_t intersect_seq_size_only(const __mram_ptr node_t *a, node_t na,
                                            const __mram_ptr node_t *b, node_t nb);

/* =========================
 *  Depth-1 worker (unchanged):
 *  A single tasklet handles one (root, second_root) pair completely.
 * ========================= */
static ans_t __imp_tri_tri6_2(sysname_t tasklet_id, node_t root, node_t second_root) {
    node_t(*tasklet_buf)[BUF_SIZE] = buf[tasklet_id];

    edge_ptr root_begin = row_ptr[root];
    edge_ptr root_end   = row_ptr[root + 1];
    edge_ptr sb         = row_ptr[second_root];
    edge_ptr se         = row_ptr[second_root + 1];

    ans_t ans = 0;

    node_t common_size = intersect_seq_buf_thresh(tasklet_buf,
                                &col_idx[root_begin],   root_end - root_begin,
                                &col_idx[sb],           se - sb,
                                mram_buf[tasklet_id],   INVALID_NODE);

    for (edge_ptr i = 0; i < common_size; i++) {
        node_t third_root = mram_buf[tasklet_id][i];
        if (third_root >= second_root) break;

        edge_ptr tb = row_ptr[third_root];
        edge_ptr te = row_ptr[third_root + 1];

        node_t common_size2 = intersect_seq_buf_thresh(tasklet_buf,
                                    &col_idx[sb],   se - sb,
                                    &col_idx[tb],   te - tb,
                                    mram_buf[tasklet_id + NR_TASKLETS], INVALID_NODE);

        node_t common_size3 = intersect_seq_buf_thresh(tasklet_buf,
                                    &col_idx[root_begin],   root_end - root_begin,
                                    &col_idx[tb],           te - tb,
                                    mram_buf[tasklet_id + (NR_TASKLETS << 1)], INVALID_NODE);

        node_t common_size123 = intersect_seq_buf_thresh(tasklet_buf,
                                    mram_buf[tasklet_id],               common_size,
                                    mram_buf[tasklet_id + NR_TASKLETS], common_size2,
                                    mram_buf[tasklet_id + (NR_TASKLETS << 1)], INVALID_NODE);

        ans += ((ans_t)(common_size - 1)) * (common_size2 - 1) * (common_size3 - 1)
             - ((ans_t)common_size123) * (common_size + common_size2 + common_size3 - 5);
    }
    return ans;
}

/* =========================
 *  Depth-2 parallel worker:
 *  - tasklet0 computes root∩second_root into shared buffer mram_buf[0][]
 *  - all tasklets scan it in parallel (stride)
 *  - tasklet0 reduces the partial sums
 * ========================= */
/* depth-2 parallel worker (FIXED to prevent WRAM overflow) */
static ans_t __imp_tri_tri6_2_parallel(sysname_t tasklet_id, node_t root, node_t second_root) {
    node_t(*tasklet_buf)[BUF_SIZE] = buf[tasklet_id];

    edge_ptr root_begin = row_ptr[root];
    edge_ptr root_end   = row_ptr[root + 1];
    edge_ptr sb         = row_ptr[second_root];
    edge_ptr se         = row_ptr[second_root + 1];

    /* tasklet0 computes root∩second into shared buffer and also gets the ACTUAL size written. */
    static node_t shared_common_size; /* WRAM scalar used to broadcast */
    if (tasklet_id == 0) {
        /* IMPORTANT: use the returned size as the true bound; it is capped by BUF_SIZE. */
        node_t written = intersect_seq_buf_thresh(buf[0],
                            &col_idx[root_begin], root_end - root_begin,
                            &col_idx[sb],         se - sb,
                            mram_buf[0],          INVALID_NODE);
        shared_common_size = written;
    }
    barrier_wait(&co_barrier);

    node_t common_size = shared_common_size;  /* everyone reads the same bound */

    ans_t partial = 0;

    for (edge_ptr i = tasklet_id; i < common_size; i += NR_TASKLETS) {
        node_t third_root = mram_buf[0][i];
        if (third_root >= second_root) continue;

        edge_ptr tb = row_ptr[third_root];
        edge_ptr te = row_ptr[third_root + 1];

        node_t common_size2 = intersect_seq_buf_thresh(tasklet_buf,
                                    &col_idx[sb],   se - sb,
                                    &col_idx[tb],   te - tb,
                                    mram_buf[tasklet_id + NR_TASKLETS], INVALID_NODE);

        node_t common_size3 = intersect_seq_buf_thresh(tasklet_buf,
                                    &col_idx[root_begin],   root_end - root_begin,
                                    &col_idx[tb],           te - tb,
                                    mram_buf[tasklet_id + (NR_TASKLETS << 1)], INVALID_NODE);

        node_t common_size123 = intersect_seq_buf_thresh(tasklet_buf,
                                    mram_buf[0],                           common_size,
                                    mram_buf[tasklet_id + NR_TASKLETS],    common_size2,
                                    mram_buf[tasklet_id + (NR_TASKLETS << 1)], INVALID_NODE);

        partial += ((ans_t)(common_size - 1)) * (common_size2 - 1) * (common_size3 - 1)
                 - ((ans_t)common_size123) * (common_size + common_size2 + common_size3 - 5);
    }

    /* reduction */
    static ans_t partial2[NR_TASKLETS];
    partial2[tasklet_id] = partial;
    barrier_wait(&co_barrier);

    ans_t sum = 0;
    if (tasklet_id == 0) {
        for (uint32_t t = 0; t < NR_TASKLETS; ++t) sum += partial2[t];
    }
    barrier_wait(&co_barrier);

    return sum;  /* only valid on tasklet0 */
}

/* =========================
 *  Original depth-1 version (kept unchanged)
 * ========================= */
static ans_t __imp_tri_tri6(sysname_t tasklet_id, node_t root) {
    edge_ptr root_begin = row_ptr[root];
    edge_ptr root_end   = row_ptr[root + 1];
    ans_t ans = 0;
    for (edge_ptr i = root_begin; i < root_end; i++) {
        node_t second_root = col_idx[i];
        if (second_root >= root) break;
        ans += __imp_tri_tri6_2(tasklet_id, root, second_root);
    }
    return ans;
}

/* =========================
 *  Buffers for heavy candidate list
 * ========================= */
static edge_ptr  heavy_local_idx[NR_TASKLETS][MAX_HEAVY_PER_TSK];
static node_t    heavy_local_cnt[NR_TASKLETS];

/* =========================
 *  Main entry: tri_tri6
 *  - depth=1 collaboration (original)
 *  - depth=2: for heavy second_root, run parallel collaboration
 * ========================= */
extern void tri_tri6(sysname_t tasklet_id) {
    static ans_t partial_ans[NR_TASKLETS];
    static uint64_t partial_cycle[NR_TASKLETS];
    static perfcounter_cycles cycles[NR_TASKLETS];

    node_t i = 0;
    while (i < root_num) {
        node_t root = roots[i];
        node_t root_begin = row_ptr[root];
        node_t root_end   = row_ptr[root + 1];

        if (root_end - root_begin < BRANCH_LEVEL_THRESHOLD) {
            break;
        }

        barrier_wait(&co_barrier);
#ifdef PERF
        timer_start(&cycles[tasklet_id]);
#endif

        partial_ans[tasklet_id]     = 0;
        heavy_local_cnt[tasklet_id] = 0;

        /* Phase A: detect heavy second_root in each tasklet's stride */
        for (edge_ptr j = root_begin + tasklet_id; j < root_end; j += NR_TASKLETS) {
            node_t second_root = col_idx[j];
            if (second_root >= root) break;

            edge_ptr sb = row_ptr[second_root];
            edge_ptr se = row_ptr[second_root + 1];

            node_t cs = intersect_seq_size_only(&col_idx[root_begin], root_end - root_begin,
                                                &col_idx[sb],         se - sb);

            if (cs >= HEAVY_T2 && heavy_local_cnt[tasklet_id] < MAX_HEAVY_PER_TSK) {
                heavy_local_idx[tasklet_id][heavy_local_cnt[tasklet_id]++] = j;
            }
        }

        barrier_wait(&co_barrier);

        /* Phase B: cooperative processing for heavy second_root */
        static node_t shared_owner_count;
        static node_t shared_second_root;

        for (uint32_t owner = 0; owner < NR_TASKLETS; ++owner) {
            /* Broadcast how many heavy items this owner has */
            if (tasklet_id == owner) {
                shared_owner_count = heavy_local_cnt[owner];
            }
            barrier_wait(&co_barrier);

            node_t count = shared_owner_count;

            for (node_t k = 0; k < count; ++k) {
                /* Owner publishes the k-th heavy second_root */
                if (tasklet_id == owner) {
                    edge_ptr j = heavy_local_idx[owner][k];
                    shared_second_root = col_idx[j];     /* MRAM read */
                }
                barrier_wait(&co_barrier);

                /* All tasklets collaboratively process this second_root */
                ans_t add = __imp_tri_tri6_2_parallel(tasklet_id, root, shared_second_root);

                /* Only tasklet 0's return value is valid; accumulate there */
                if (tasklet_id == 0) {
                    partial_ans[0] += add;
                }
                barrier_wait(&co_barrier);
            }
        }

        /* Phase C: process light second_root independently */
        for (edge_ptr j = root_begin + tasklet_id; j < root_end; j += NR_TASKLETS) {
            node_t second_root = col_idx[j];
            if (second_root >= root) break;

            int is_heavy = 0;
            for (node_t k = 0; k < heavy_local_cnt[tasklet_id]; ++k) {
                if (heavy_local_idx[tasklet_id][k] == j) { is_heavy = 1; break; }
            }
            if (is_heavy) continue;

            partial_ans[tasklet_id] += __imp_tri_tri6_2(tasklet_id, root, second_root);
        }

#ifdef PERF
        partial_cycle[tasklet_id] = timer_stop(&cycles[tasklet_id]);
#endif
        barrier_wait(&co_barrier);

        /* Reduction */
        if (tasklet_id == 0) {
            ans_t total_ans = 0;
#ifdef PERF
            uint64_t total_cycle = 0;
#endif
            for (uint32_t j = 0; j < NR_TASKLETS; j++) {
                total_ans += partial_ans[j];
#ifdef PERF
                total_cycle += partial_cycle[j];
#endif
            }
            ans[i] = total_ans;
#ifdef PERF
            cycle_ct[i] = total_cycle;
#endif
        }
        i++;
    }

    /* Fallback: distribute remaining roots in round-robin (unchanged) */
    for (i += tasklet_id; i < root_num; i += NR_TASKLETS) {
        node_t root = roots[i];
#ifdef PERF
        timer_start(&cycles[tasklet_id]);
#endif
        ans[i] = __imp_tri_tri6(tasklet_id, root);
#ifdef PERF
        cycle_ct[i] = timer_stop(&cycles[tasklet_id]);
#endif
    }
}

/* =========================
 *  Intersection size-only version (two-pointer)
 * ========================= */
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
