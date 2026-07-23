// Sshwifty - A Web SSH client
//
// Copyright (C) 2019-2022 Ni Rui <ranqus@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

import assert from "assert";
import { hasDirectConnectTarget } from "./direct_connect.js";

describe("Direct connect", () => {
  it("accepts direct connect targets", () => {
    assert.strictEqual(hasDirectConnectTarget("+192.168.1.10"), true);
    assert.strictEqual(hasDirectConnectTarget("+device"), true);
  });

  it("rejects empty or invalid targets", () => {
    assert.strictEqual(hasDirectConnectTarget(""), false);
    assert.strictEqual(hasDirectConnectTarget("+"), false);
    assert.strictEqual(hasDirectConnectTarget("device"), false);
    assert.strictEqual(hasDirectConnectTarget("   "), false);
  });
});
