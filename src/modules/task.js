// SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

export function createQueryTask(method, query, options) {
  return function(callback) {
    return method(query, options, callback)
  }
}

export function runQueryTask(task, callback) {
  if (callback) {
    task(callback)
  } else {
    try {
      var result = task()
      return result
    } catch (e) {
      throw e
    }
  }
}
