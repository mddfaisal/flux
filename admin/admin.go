package admin

import (
	"encoding/json"
	"html/template"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mddfaisal/flux/server/quashserver"
	"github.com/mddfaisal/flux/structs"
	"github.com/mddfaisal/flux/utils"
)

type PageData struct {
	Title string
}

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	tmpl = template.Must(template.New("index").Parse(`
<!DOCTYPE html>
<html>
<title>{{.Title}}</title>
	<style>` + utils.W3_CSS + `</style>
	<script type="text/javascript">` + utils.JQuery + `</script>
	<script type="text/javascript">` + utils.Loder_Js + `</script>
	<script type="text/javascript">
		google.charts.load('current', {'packages':['gauge']});
      	google.charts.setOnLoadCallback(drawSystemRamUsage);
      	google.charts.setOnLoadCallback(drawCPUUsage);
      	google.charts.setOnLoadCallback(drawFluxRamUsage);
		const ws = new WebSocket("ws://localhost:6301/ws/admin");
		ws.onmessage = (e) => {
			const metrics = JSON.parse(e.data);
			console.log("live queue metrics:", metrics);
			if (metrics.system_ram_usage) {
				drawSystemRamUsage(metrics.system_ram_usage)
			}
			if (metrics.heap_alloc_mb) {
				drawFluxRamUsage(metrics.heap_alloc_mb)
			}
			if (metrics.cpu_usage) {
				drawCPUUsage(metrics.cpu_usage)
			}
			document.getElementById("process_info").innerHTML =
			'<p>Total Alloc MB: '+metrics.total_alloc_mb+' | SysMB: '+metrics.sys_mb+' | NumGC: '+metrics.num_gc+'</p>';
		};
		function drawSystemRamUsage(e) {
			var data = google.visualization.arrayToDataTable([
				['Label', 'Value'],
				['RAM(%)', e],
			]);
			var options = {
				width: 200, height: 200,
				redFrom: 80, redTo: 100,
				yellowFrom:75, yellowTo: 80,
				minorTicks: 5
			};
			var chart = new google.visualization.Gauge(document.getElementById('system_ram_usage'));
			chart.draw(data, options);
		}
		function drawFluxRamUsage(e) {
			var data = google.visualization.arrayToDataTable([
				['Label', 'Value'],
				['Flux heap (MB)', e],
			]);
			var options = {
				width: 200, height: 200,
				redFrom: 400, redTo: 512,
				yellowFrom: 250, yellowTo: 400,
				minorTicks: 5,
				max: 512
			};
			var chart = new google.visualization.Gauge(document.getElementById('flux_memory_usage'));
			chart.draw(data, options);
		}
		function drawCPUUsage(e) {
			var data = google.visualization.arrayToDataTable([
				['Label', 'Value'],
				['CPU(%)', e],
			]);
			var options = {
				width: 200, height: 200,
				redFrom: 80, redTo: 100,
				yellowFrom:75, yellowTo: 80,
				minorTicks: 5
			};
			var chart = new google.visualization.Gauge(document.getElementById('cpu_usage'));
			chart.draw(data, options);
		}
  	</script>
<body>
	<div class="w3-container w3-blue">
		<h1>{{.Title}}</h1>
	</div>
	<div class="w3-container">
		<div class="w3-grid" style="grid-template-columns:3fr 1fr">
			<div class="w3-container">
				<div class="w3-panel w3-border">
					<h5><b>Key Value Pair Operations</b></h5>
				</div>
				<div class="w3-panel w3-border">
					<h5><b>Pub/Sub Operations</b></h5>
				</div>
				<div class="w3-panel w3-border">
					<h5><b>Pub/Sub Metrics</b></h5>
				</div>
			</div>
			<div class="w3-container">
				<div class="w3-panel w3-border">
					<div id="system_ram_usage"></div>
					<p>System Ram Usage</p>
				</div>
				<div class="w3-panel w3-border">
					<div id="flux_memory_usage"></div>
					<div id="process_info"></div>
					<p>Flux Memory Usage</p>
				</div>
				<div class="w3-panel w3-border">
					<div id="cpu_usage"></div>
					<p>CPU Usage</p>
				</div>
			</div>
		</div>
	</div>
</body>
</html>
`))
)

func websocketHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("ws upgrade error:", err)
		return
	}
	defer conn.Close()

	for {
		srvData := quashserver.Snapshot()
		resp := structs.AdminResponse{
			QueueMetric: srvData,
		}
		data, _ := json.Marshal(resp.QueueMetric)
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			log.Println("ws write error:", err)
			return
		}
		time.Sleep(500 * time.Millisecond) // throttle push rate
	}
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	data := PageData{Title: "Quash Admin"}
	tmpl.Execute(w, data)
}

func AdminServer() {
	http.HandleFunc("/ws/admin", websocketHandler)
	http.HandleFunc("/", indexHandler)
	log.Println("admin server listening on", utils.QuashTelemetry)
	http.ListenAndServe(utils.QuashTelemetry, nil)
}
