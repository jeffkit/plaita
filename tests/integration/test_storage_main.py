"""
存储模块测试主入口
运行此文件可以执行所有存储模块的测试
"""
import unittest
import sys
import os

# 确保仓库根在 sys.path 上 (让 ``tests.`` 包可被 import)
current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(os.path.dirname(current_dir))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# 导入测试模块 (2026-07 归类后这些 test_storage* 都搬到 tests/integration/)
from tests.integration.test_storage import TestStateStorageBase, TestMemoryStateStorage
from tests.integration.test_storage_commons import get_standard_test_cases
from plaita.storage import MemoryExecutionStorage

# 尝试导入Redis相关测试
try:
    from tests.integration.test_storage_redis import (
        TestRedisStateStorage, StandardRedisStateStorageTests,
    )
    REDIS_TESTS_AVAILABLE = True
except ImportError:
    REDIS_TESTS_AVAILABLE = False


def get_test_suite():
    """创建测试套件"""
    test_suite = unittest.TestSuite()
    
    # 基础测试
    test_suite.addTest(unittest.makeSuite(TestStateStorageBase))
    
    # 内存存储测试
    test_suite.addTest(unittest.makeSuite(TestMemoryStateStorage))
    
    # 生成标准内存存储测试
    StandardMemoryTests = get_standard_test_cases(MemoryExecutionStorage)
    test_suite.addTest(unittest.makeSuite(StandardMemoryTests))
    
    # Redis存储测试（如果可用）
    if REDIS_TESTS_AVAILABLE:
        test_suite.addTest(unittest.makeSuite(TestRedisStateStorage))
        test_suite.addTest(unittest.makeSuite(StandardRedisStateStorageTests))
    
    return test_suite


if __name__ == "__main__":
    # 创建测试套件
    suite = get_test_suite()
    
    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # 确定退出码
    sys.exit(not result.wasSuccessful()) 