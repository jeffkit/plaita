import json
import unittest
import http.server
import threading
import socketserver
import time
from unittest.mock import MagicMock, patch

from plaita.node.http import (
    HTTP, 
    HttpResponse, 
    HttpExecutor, 
    HTTP_GEN_REQUEST_ERROR, 
    HTTP_DO_REQUEST_ERROR, 
    HTTP_NODE_EXEC_ERROR,
    RESPONSE_DATA_KEY,
    RESPONSE_CTX_KEY,
    STATUS_CTX_KEY,
    HEADER_CTX_KEY
)
from plaita.core.errors import NodeException
from plaita import Flow


class MockResponse:
    """Mock HTTP响应对象"""
    def __init__(self, status_code=200, content=None, json_data=None, headers=None, reason="OK"):
        self.status_code = status_code
        self.reason = reason
        self._content = content or b'{"message": "success"}'
        self._json = json_data or {"message": "success"}
        self.headers = headers or {"Content-Type": "application/json"}
        
    def json(self):
        return self._json
        
    @property
    def text(self):
        return self._content.decode('utf-8') if isinstance(self._content, bytes) else self._content


class MockExecution:
    """Mock执行上下文（对齐 FlowExecution 公开 API：set_state + express_prefix）"""
    def __init__(self):
        self._context = {}
        self._node_context = {}
        self.express_prefix = "$"

    def set_state(self, key, value):
        self._node_context[key] = value
    
    def evaluate(self, expression):
        """模拟表达式解析"""
        if expression == "https://example.com/api":
            return "https://example.com/api"
        elif expression == "get-data":
            return {"id": 123}
        elif expression == {"test": "value"}:
            return {"test": "value"}
        elif expression == {"Authorization": "Bearer token123"}:
            return {"Authorization": "Bearer token123"}
        elif isinstance(expression, dict):
            # 对于字典类型的表达式，直接返回相同的字典
            return expression
        else:
            return expression
            
    def set_node_context(self, node_id, key, value):
        """设置节点上下文"""
        if node_id not in self._node_context:
            self._node_context[node_id] = {}
        self._node_context[node_id][key] = value


class TestHTTPNode(unittest.TestCase):
    """HTTP节点单元测试"""
    
    def setUp(self):
        """每个测试前的设置"""
        self.http_node = HTTP(
            id="test_http",
            url="https://example.com/api",
            method="GET"
        )
        self.execution = MockExecution()
    
    def test_init(self):
        """测试初始化"""
        self.assertEqual(self.http_node.id, "test_http")
        self.assertEqual(self.http_node.url, "https://example.com/api")
        self.assertEqual(self.http_node.method, "GET")
        self.assertEqual(self.http_node.content_type, "application/json")
        
    def test_get_type(self):
        """测试获取节点类型"""
        self.assertEqual(self.http_node.get_type(), "http")
        
    def test_validate_success(self):
        """测试验证成功"""
        try:
            self.http_node.validate()
            # 如果没有抛出异常，则测试通过
            success = True
        except Exception:
            success = False
        self.assertTrue(success)
        
    def test_validate_failure_no_url(self):
        """测试验证失败 - 无URL"""
        node = HTTP(
            id="test_http",
            url="placeholder",  # 提供一个占位URL，防止验证错误
            method="GET"
        )
        node.url = ""  # 设置为空字符串
        with self.assertRaises(ValueError):
            node.validate()
            
    def test_validate_failure_invalid_method(self):
        """测试验证失败 - 无效的HTTP方法

        2026-09 起 method 为 Literal 声明：非法值在构造（解析期）即被拒绝，
        不再等到 validate()。
        """
        with self.assertRaises(Exception):
            HTTP(id="test_http", url="https://example.com", method="INVALID")
            
    @patch("requests.Session.send")
    def test_execute_success(self, mock_send):
        """测试成功执行HTTP请求"""
        # 准备模拟响应
        mock_response = MockResponse(
            status_code=200,
            json_data={"result": "success", "data": {"id": 123}}
        )
        mock_send.return_value = mock_response
        
        # 创建一个模拟请求
        mock_request = MagicMock()
        
        # 模拟HttpExecutor.handle_request方法，直接返回成功响应
        def mock_handle_request(ctx):
            return HttpResponse(
                raw_request=mock_request,
                raw_response=mock_response,
                res={"result": "success", "data": {"id": 123}}
            ), None
        
        # 使用补丁替换handle_request方法
        with patch.object(HttpExecutor, 'handle_request', side_effect=mock_handle_request):
            # 执行测试
            result = self.http_node.execute(self.execution)
            
            # 验证结果
            self.assertEqual(result, {"result": "success", "data": {"id": 123}})
        
        mock_send.assert_not_called()  # 因为我们模拟了handle_request方法
        
    @patch("requests.Session.send")
    def test_execute_with_output(self, mock_send):
        """测试带输出表达式的HTTP请求"""
        # 准备模拟响应
        mock_response = MockResponse(
            status_code=200,
            json_data={"result": "success", "data": {"id": 123}}
        )
        mock_send.return_value = mock_response
        
        # 设置输出表达式
        self.http_node.output = "get-data"
        
        # 创建一个模拟请求
        mock_request = MagicMock()
        
        # 模拟HttpExecutor.handle_request方法，直接返回成功响应
        def mock_handle_request(ctx):
            return HttpResponse(
                raw_request=mock_request,
                raw_response=mock_response,
                res={"result": "success", "data": {"id": 123}}
            ), None
        
        # 使用补丁替换handle_request方法
        with patch.object(HttpExecutor, 'handle_request', side_effect=mock_handle_request):
            # 执行测试
            result = self.http_node.execute(self.execution)
            
            # 验证结果
            self.assertEqual(result, {"id": 123})
        
        mock_send.assert_not_called()  # 因为我们模拟了handle_request方法
        
    @patch("requests.Session.send")
    def test_execute_http_error(self, mock_send):
        """测试HTTP请求错误"""
        # 设置模拟发送请求时抛出异常
        mock_send.side_effect = Exception("Connection error")
        
        # 创建一个模拟请求，这样会触发HTTP_DO_REQUEST_ERROR
        mock_request = MagicMock()
        with patch.object(HttpExecutor, 'new_request', return_value=mock_request):
            # 执行测试——错误路径现在 raise（曾经把 NodeException 当返回值，
            # 导致 errorHandler 的 continue/continue_with 永不生效）
            with self.assertRaises(NodeException) as cm:
                self.http_node.execute(self.execution)

            # 验证结果
            self.assertEqual(cm.exception.code, HTTP_DO_REQUEST_ERROR)
            self.assertIn("Connection error", cm.exception.message)
            
        mock_send.assert_called_once()
        
    def test_parse_url(self):
        """测试URL解析"""
        # 模拟evaluate函数
        with patch('plaita.node.http.evaluate') as mock_evaluate:
            # 设置模拟返回值
            mock_evaluate.return_value = "https://example.com/api"
            
            # 调用测试方法
            url = self.http_node.parse_url(self.execution)
            
            # 验证结果
            self.assertEqual(url, "https://example.com/api")
            mock_evaluate.assert_called_once()
        
    def test_parse_header(self):
        """测试请求头解析"""
        # 模拟evaluate函数
        with patch('plaita.node.http.evaluate') as mock_evaluate:
            # 设置模拟返回值
            test_headers = {"Authorization": "Bearer token123"}
            mock_evaluate.return_value = test_headers
            
            # 设置请求头
            self.http_node.headers = {"Authorization": "Bearer token123"}
            
            # 调用测试方法
            headers = self.http_node.parse_header(self.execution)
            
            # 验证结果
            self.assertEqual(headers["Content-Type"], "application/json")
            self.assertEqual(headers["Authorization"], "Bearer token123")
            mock_evaluate.assert_called_once()
        
    def test_parse_query(self):
        """测试查询参数解析"""
        # 模拟evaluate函数
        with patch('plaita.node.http.evaluate') as mock_evaluate:
            # 设置模拟返回值
            test_query = {"page": 1, "limit": 10}
            mock_evaluate.return_value = test_query
            
            # 设置查询参数
            self.http_node.query = {"page": 1, "limit": 10}
            
            # 调用测试方法
            query = self.http_node.parse_query(self.execution)
            
            # 验证结果
            self.assertEqual(query, test_query)
            mock_evaluate.assert_called_once()
        
    def test_parse_body(self):
        """测试请求体解析"""
        # 模拟evaluate函数
        with patch('plaita.node.http.evaluate') as mock_evaluate:
            # 设置模拟返回值
            test_body = {"test": "value"}
            mock_evaluate.return_value = test_body
            
            # 设置请求体
            self.http_node.body = {"test": "value"}
            
            # 调用测试方法
            body = self.http_node.parse_body(self.execution)
            
            # 验证结果
            self.assertEqual(body, test_body)
            mock_evaluate.assert_called_once()
        
    @patch("requests.Session.send")
    def test_post_request(self, mock_send):
        """测试POST请求"""
        # 准备HTTP节点
        post_node = HTTP(
            id="test_post",
            url="https://example.com/api",
            method="POST",
            body={"name": "Test User", "email": "test@example.com"}
        )
        
        # 准备模拟响应
        mock_response = MockResponse(
            status_code=201,
            json_data={"id": 123, "name": "Test User", "email": "test@example.com"}
        )
        
        # 创建一个模拟请求
        mock_request = MagicMock()
        
        # 模拟HttpExecutor.handle_request方法，直接返回成功响应
        def mock_handle_request(ctx):
            return HttpResponse(
                raw_request=mock_request,
                raw_response=mock_response,
                res={"id": 123, "name": "Test User", "email": "test@example.com"}
            ), None
        
        # 使用补丁替换handle_request方法
        with patch.object(HttpExecutor, 'handle_request', side_effect=mock_handle_request):
            # 执行测试
            result = post_node.execute(self.execution)
            
            # 验证结果
            self.assertEqual(result["id"], 123)
            self.assertEqual(result["name"], "Test User")
        
        mock_send.assert_not_called()  # 因为我们模拟了handle_request方法
        
    @patch("requests.Session.send")
    def test_handle_request_json_parse_error(self, mock_send):
        """测试处理非JSON响应"""
        # 准备模拟响应
        mock_response = MockResponse(
            status_code=200,
            content=b"Plain text response"
        )
        # 修改json方法使其抛出异常
        mock_response.json = MagicMock(side_effect=json.JSONDecodeError("Invalid JSON", "", 0))
        
        # 创建一个模拟请求
        mock_request = MagicMock()
        
        # 模拟HttpExecutor.handle_request方法，直接返回文本响应
        def mock_handle_request(ctx):
            return HttpResponse(
                raw_request=mock_request,
                raw_response=mock_response,
                res="Plain text response"
            ), None
        
        # 使用补丁替换handle_request方法
        with patch.object(HttpExecutor, 'handle_request', side_effect=mock_handle_request):
            # 执行测试
            result = self.http_node.execute(self.execution)
            
            # 验证结果应该是文本字符串
            self.assertEqual(result, "Plain text response")
        
        mock_send.assert_not_called()  # 因为我们模拟了handle_request方法
        
    @patch("requests.Request.prepare")
    def test_executor_new_request(self, mock_prepare):
        """测试HTTP请求创建"""
        # 模拟准备好的请求
        mock_prepared_request = MagicMock()
        mock_prepared_request.method = "GET"
        mock_prepared_request.headers = {"Accept": "application/json"}
        mock_prepare.return_value = mock_prepared_request
        
        # 创建执行器
        executor = HttpExecutor(
            url="https://example.com/api",
            method="GET",
            query={"q": "test"},
            body=None,
            headers={"Accept": "application/json"},
            addressing=None,
            delegate=None
        )
        
        # 生成请求
        request = executor.new_request(self.execution)
        
        # 验证请求
        self.assertEqual(request.method, "GET")
        self.assertEqual(request.headers["Accept"], "application/json")
        mock_prepare.assert_called_once()
        
    def test_http_response(self):
        """测试HTTP响应对象"""
        # 创建请求和响应
        request = MagicMock()
        response = MagicMock()
        response.status_code = 200
        
        # 创建HTTP响应对象
        http_response = HttpResponse(
            raw_request=request,
            raw_response=response,
            res={"result": "success"}
        )
        
        # 验证方法
        self.assertFalse(http_response.empty())
        self.assertFalse(http_response.send_request_fail())
        
        # 测试只有请求没有响应的情况
        http_response = HttpResponse(raw_request=request)
        self.assertTrue(http_response.send_request_fail())


class TestHTTPNodeAdditional(unittest.TestCase):
    """HTTP节点额外测试"""
    
    def setUp(self):
        """每个测试前的设置"""
        self.http_server = self.start_test_server()
        self.server_port = self.http_server.server_port
        self.base_url = f"http://localhost:{self.server_port}"
        
    def tearDown(self):
        """每个测试后的清理"""
        self.stop_test_server()
    
    def start_test_server(self):
        """启动测试HTTP服务器"""
        class TestRequestHandler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == '/api/v1/foo':
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({"foo": "bar"}).encode('utf-8'))
                else:
                    self.send_response(404)
                    self.end_headers()
            
            def do_POST(self):
                if self.path.startswith('/api/v1/bar'):
                    content_length = int(self.headers.get('Content-Length', 0))
                    body = self.rfile.read(content_length).decode('utf-8')
                    
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    response = {"foo": "bar", "age": 20, "country": "China"}
                    self.wfile.write(json.dumps(response).encode('utf-8'))
                else:
                    self.send_response(404)
                    self.end_headers()
                    
            def log_message(self, format, *args):
                # 禁止日志输出
                pass
                
        class TestServer:
            def __init__(self):
                self.started = False
                self.server_port = 0
                
            def start(self):
                """启动服务器"""
                # 使用0获取一个随机可用端口
                self.server = socketserver.TCPServer(("localhost", 0), TestRequestHandler)
                self.server_port = self.server.server_address[1]
                self.server_thread = threading.Thread(target=self.server.serve_forever)
                self.server_thread.daemon = True
                self.server_thread.start()
                self.started = True
                # 给服务器一点启动时间
                time.sleep(0.1)
                
            def stop(self):
                """停止服务器"""
                if self.started:
                    self.server.shutdown()
                    self.server.server_close()
                    self.started = False
        
        # 创建并启动服务器
        server = TestServer()
        server.start()
        return server
    
    def stop_test_server(self):
        """停止测试服务器"""
        if hasattr(self, 'http_server'):
            self.http_server.stop()
    
    # 模拟代理类
    class MockDelegate:
        def do(self, request, params):
            """模拟代理方法"""
            response = MagicMock()
            response.status_code = 200
            response.reason = "OK"
            response.headers = {"Content-Type": "application/json"}
            response.json.return_value = {"user": "nick"}
            response.text = json.dumps({"user": "nick"})
            return response, None
    
    def test_json_unmarshal(self):
        """测试JSON解析"""
        # 准备测试数据
        json_data = """
        {
            "id": "http",
            "type": "http", 
            "next": "end",
            "url": "http://example.com/api/v1/foo",
            "method": "GET", 
            "headers": {
                "Content-Type": "application/json"
            },
            "body": {
                "key": "value",
                "key2": [1, 2]
            },
            "query": {
                "foo": "bar"
            }
        }
        """
        
        # 从JSON创建HTTP节点
        http_node = HTTP.model_validate_json(json_data)
        
        # 验证解析结果
        self.assertEqual(http_node.id, "http")
        self.assertEqual(http_node.method, "GET")
        self.assertEqual(http_node.url, "http://example.com/api/v1/foo")
        self.assertEqual(http_node.content_type, "application/json")
        self.assertEqual(http_node.query, {"foo": "bar"})
        self.assertEqual(http_node.body, {"key": "value", "key2": [1, 2]})
        self.assertEqual(http_node.headers, {"Content-Type": "application/json"})
        
    def test_query_merge(self):
        """测试查询参数合并"""
        # 创建HTTP节点
        http_node = HTTP(
            id="http_query",
            url=f"{self.base_url}/api/v1/foo?existing=param",
            method="GET",
            query={"additional": "query"}
        )
        
        # 模拟执行上下文
        execution = MagicMock()
        
        # 模拟HttpExecutor的方法
        with patch.object(HttpExecutor, 'handle_request') as mock_handle_request:
            # 设置模拟响应
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.reason = "OK"
            mock_response.headers = {"Content-Type": "application/json"}
            
            # 模拟请求对象
            mock_request = MagicMock()
            
            def side_effect(ctx):
                # 返回成功响应
                return HttpResponse(
                    raw_request=mock_request,
                    raw_response=mock_response,
                    res={"foo": "bar"}
                ), None
                
            mock_handle_request.side_effect = side_effect
            
            # 使用patch来模拟evaluate函数的返回值
            with patch('plaita.node.http.evaluate', side_effect=lambda ctx, expr: 
                  f"{self.base_url}/api/v1/foo?existing=param" if expr == http_node.url
                  else {"additional": "query"} if expr == http_node.query
                  else expr):
                result = http_node.execute(execution)
                    
            # 验证结果
            self.assertEqual(result, {"foo": "bar"})
            mock_handle_request.assert_called_once()
    
    def test_response_context(self):
        """测试响应上下文"""
        # 创建HTTP节点
        http_node = HTTP(
            id="http_ctx",
            url=f"{self.base_url}/api/v1/foo",
            method="GET",
            output="$RESPONSE.data.foo"  # 使用RESPONSE上下文
        )
        
        # 模拟执行上下文
        execution = MagicMock()
        execution.express_prefix = "$"  # MagicMock 默认属性会让 f-string 拼出垃圾键
        execution.evaluate.side_effect = lambda expr: "bar" if expr == "$RESPONSE.data.foo" else expr
        
        # 模拟HttpExecutor的方法
        with patch.object(HttpExecutor, 'handle_request') as mock_handle_request:
            # 设置模拟响应
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.reason = "OK"
            mock_response.headers = {"Content-Type": "application/json"}
            
            # 模拟请求对象
            mock_request = MagicMock()
            
            def side_effect(ctx):
                # 返回成功响应
                return HttpResponse(
                    raw_request=mock_request,
                    raw_response=mock_response,
                    res={"foo": "bar"}
                ), None
                
            mock_handle_request.side_effect = side_effect
            
            # 执行测试
            result = http_node.execute(execution)
            
            # 验证结果
            self.assertEqual(result, "bar")
            
            # 验证上下文设置（公开 API set_state，键为 $NODE.<id>.<KEY>）
            execution.set_state.assert_any_call("$NODE.http_ctx.RESPONSE", {
                "data": {"foo": "bar"},
                "status": 200,
                "statusText": "OK",
                "headers": mock_response.headers
            })
            execution.set_state.assert_any_call("$NODE.http_ctx.STATUS", 200)
    
    @patch('plaita.node.http.HTTP.new_executor')
    def test_delegate_method(self, mock_new_executor):
        """测试代理方法"""
        # 创建HTTP节点
        http_node = HTTP(
            id="http_delegate",
            url=f"{self.base_url}/api/v1/bar",
            method="POST",
            delegate={"name": "mockBar", "params": None},
            output="$RESPONSE.data.user"
        )
        
        # 模拟执行上下文
        execution = MagicMock()
        execution.evaluate.side_effect = lambda expr: "nick" if expr == "$RESPONSE.data.user" else expr
        
        # 模拟响应
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.reason = "OK"
        mock_response.headers = {"Content-Type": "application/json"}
        
        # 模拟请求
        mock_request = MagicMock()
        
        # 模拟执行器
        mock_executor = MagicMock()
        mock_executor.handle_request.return_value = (
            HttpResponse(
                raw_request=mock_request,
                raw_response=mock_response,
                res={"user": "nick"}
            ), 
            None
        )
        mock_new_executor.return_value = mock_executor
        
        # 执行测试
        result = http_node.execute(execution)
        
        # 验证结果
        self.assertEqual(result, "nick")
        mock_executor.handle_request.assert_called_once()
    
    def test_integration_with_real_server(self):
        """与真实服务器集成测试"""
        # 创建HTTP节点
        http_node = HTTP(
            id="http_integration",
            url=f"{self.base_url}/api/v1/foo",
            method="GET"
        )
        
        # 模拟执行上下文
        class SimpleExecution:
            def __init__(self):
                self.node_context = {}
                self.express_prefix = "$"

            def evaluate(self, expr):
                return expr

            def set_state(self, key, value):
                self.node_context[key] = value
        
        execution = SimpleExecution()
        
        # 执行实际HTTP请求
        # evaluate 真实签名是 (value, context)——http 节点 0.5.x 修复了传参颠倒
        with patch('plaita.node.http.evaluate', side_effect=lambda expr, ctx: f"{self.base_url}/api/v1/foo" if expr == http_node.url else expr):
            result = http_node.execute(execution)
            
        # 验证结果
        self.assertEqual(result, {"foo": "bar"})


if __name__ == "__main__":
    unittest.main() 

class TestHTTPNodeRealEndToEnd(unittest.TestCase):
    """真实链路 e2e：本地起 http.server，**全程零 mock**——锁定 evaluate 参数、
    set_state 响应写入、errorHandler 兜底三条真实路径（R2/R3 评审曾发现这三条
    被全 mock 的集成测试掩盖）。"""

    @classmethod
    def setUpClass(cls):
        import http.server
        import threading

        class _Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                body = json.dumps({"hello": "world", "path": self.path}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_DELETE(self):
                self.send_response(405)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *args):
                pass

        cls._server = http.server.HTTPServer(("127.0.0.1", 0), _Handler)
        cls.port = cls._server.server_address[1]
        cls._thread = threading.Thread(target=cls._server.serve_forever, daemon=True)
        cls._thread.start()

    @classmethod
    def tearDownClass(cls):
        cls._server.shutdown()
        cls._server.server_close()

    def _http_node_flow(self, url_expr, error_handler=None):
        node = {"type": "http", "id": "fetch", "method": "GET", "url": url_expr, "next": "e"}
        if error_handler:
            node["errorHandler"] = error_handler
        return json.dumps({
            "flow_id": "real_http",
            "nodes": [
                {"type": "start", "id": "s", "next": "fetch"},
                node,
                {"type": "end", "id": "e", "output": "$NODE.fetch", "resultType": "success"},
            ],
        })

    def test_real_get_request_succeeds(self):
        """真实 GET 全链路：表达式 URL 求值 → 请求 → 响应写入 $NODE.<id>.*。"""
        flow = Flow.from_string(self._http_node_flow(f"http://127.0.0.1:{self.port}/api/ping"))
        result = flow.run()
        # 无 output 的 http 节点返回 response.data 本身
        self.assertEqual(result, {"hello": "world", "path": "/api/ping"})

    def test_real_request_error_uses_error_handler_default(self):
        """不可路由端口 → errorHandler continue_with 的 defaultValue 生效。"""
        flow = Flow.from_string(self._http_node_flow(
            f"http://127.0.0.1:1/dead",
            error_handler={"strategy": "continue_with", "defaultValue": "fallback"},
        ))
        self.assertEqual(flow.run(), "fallback")

    def test_real_http_error_status_is_a_response_not_a_failure(self):
        """405 响应属于"正常响应"而非节点失败：http 节点只对传输层错误抛错
        （连接拒绝/超时等，见 test_real_request_error_uses_error_handler_default）。
        流程照常推进到 End。"""
        flow = Flow.from_string(json.dumps({
            "flow_id": "real_http_405",
            "nodes": [
                {"type": "start", "id": "s", "next": "fetch"},
                {"type": "http", "id": "fetch", "method": "DELETE",
                 "url": f"http://127.0.0.1:{self.port}/api/x", "next": "e"},
                {"type": "end", "id": "e", "output": "1"},
            ],
        }))
        self.assertEqual(flow.run(), "1")
