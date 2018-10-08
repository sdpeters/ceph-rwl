// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/BlockGuard.h"
#include "common/dout.h"
#include <algorithm>
#include <unordered_map>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::BlockGuard: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {
namespace cache {

BlockGuard::BlockGuard(CephContext *cct, uint32_t max_blocks,
                       uint32_t block_size)
  : m_cct(cct), m_max_blocks(max_blocks), m_block_size(block_size),
    m_lock("librbd::cache::BlockGuard::m_lock"),
    m_detained_block_pool(max_blocks),
    m_detained_blocks_buckets(max_blocks),
    m_detained_blocks(BlockToDetainedBlocks::bucket_traits(
      &m_detained_blocks_buckets.front(), max_blocks)) {
  for (auto &detained_block : m_detained_block_pool) {
    m_free_detained_blocks.push_back(detained_block);
  }
}

int BlockGuard::detain(uint64_t block, BlockIO *block_io) {
  Mutex::Locker locker(m_lock);
  ldout(m_cct, 20) << "block=" << block << ", "
                   << "free_slots=" << m_free_detained_blocks.size() << dendl;
  assert(block_io == nullptr || block == block_io->block);

  DetainedBlock *detained_block;
  auto detained_block_it = m_detained_blocks.find(block);
  if (detained_block_it != m_detained_blocks.end()) {
    // request against an already detained block
    detained_block = &(*detained_block_it);
    if (block_io == nullptr) {
      // policy is attempting to release this (busy) block
      return -EBUSY;
    }

    if (block_io != nullptr) {
      detained_block->block_ios.emplace_back(std::move(*block_io));
    }

    // alert the caller that the IO was detained
    return 1;
  } else {
    if (m_free_detained_blocks.empty()) {
      ldout(m_cct, 20) << "no free detained block cells" << dendl;
      return -ENOMEM;
    }

    detained_block = &m_free_detained_blocks.front();
    m_free_detained_blocks.pop_front();

    detained_block->block = block;
    m_detained_blocks.insert(*detained_block);
    return 0;
  }
}

int BlockGuard::release(uint64_t block, BlockIOs *block_ios) {
  Mutex::Locker locker(m_lock);
  auto detained_block_it = m_detained_blocks.find(block);
  assert(detained_block_it != m_detained_blocks.end());

  auto &detained_block = *detained_block_it;
  ldout(m_cct, 20) << "block=" << block << ", "
                   << "pending_ios="
                   << (detained_block.block_ios.empty() ?
                        0 : detained_block.block_ios.size() - 1) << ", "
                   << "free_slots=" << m_free_detained_blocks.size() << dendl;

  if (!detained_block.block_ios.empty()) {
    block_ios->push_back(std::move(detained_block.block_ios.front()));
    detained_block.block_ios.pop_front();
  } else {
    m_detained_blocks.erase(detained_block_it);
    m_free_detained_blocks.push_back(detained_block);
  }
  return 0;
}

BlockGuard::C_BlockRequest::C_BlockRequest(Context *on_finish)
  : lock("librbd::cache::BlockGuard::C_BlockRequest::lock"),
    on_finish(on_finish) {
}

void BlockGuard::C_BlockRequest::activate() {
  int r;
  {
    Mutex::Locker locker(lock);
    activated = true;
    if (pending_requests > 0) {
      return;
    }
    r = ret_val;
  }
  complete(r);
}

void BlockGuard::C_BlockRequest::fail(int r) {
  {
    Mutex::Locker locker(lock);
    if (ret_val >= 0 && r < 0) {
      ret_val = r;
    }
  }
  activate();
}

void BlockGuard::C_BlockRequest::add_request() {
  Mutex::Locker locker(lock);
  ++pending_requests;
}

void BlockGuard::C_BlockRequest::complete_request(int r) {
  {
    Mutex::Locker locker(lock);
    if (ret_val >= 0 && r < 0) {
      ret_val = r;
    }

    if (--pending_requests > 0 || !activated) {
      return;
    }
    r = ret_val;
  }
  complete(r);
}

void BlockGuard::C_BlockRequest::finish(int r) {
  on_finish->complete(r);
}

} // namespace cache
} // namespace librbd
