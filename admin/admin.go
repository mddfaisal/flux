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
		const ws = new WebSocket("ws://localhost:6301/ws/admin");
		ws.onmessage = (e) => {
			const metrics = JSON.parse(e.data);
			console.log("live queue metrics:", metrics);
		};
		google.charts.load('current', {'packages':['gauge']});
      	google.charts.setOnLoadCallback(drawChart);
		function drawSystemRamUsage() {}
		function drawCPUUsage() {}
		function drawFluxRamUsage() {}
		function drawChart() {
			var data = google.visualization.arrayToDataTable([
				['Label', 'Value'],
				['RAM', 80],
				['CPU', 55],
			]);
			var options = {
				// width: 600, height: 400,
				redFrom: 80, redTo: 100,
				yellowFrom:75, yellowTo: 80,
				minorTicks: 5
			};
			var chart = new google.visualization.Gauge(document.getElementById('chart_div'));
			chart.draw(data, options);
			setInterval(function() {
				data.setValue(0, 1, 40 + Math.round(60 * Math.random()));
				chart.draw(data, options);
			}, 13000);
			setInterval(function() {
				data.setValue(1, 1, 40 + Math.round(60 * Math.random()));
				chart.draw(data, options);
			}, 5000);
      	}
  	</script>
<body>
	<div class="w3-container w3-blue">
		<h1>{{.Title}}</h1>
	</div>
	<div class="w3-container">
		<div class="w3-grid" style="grid-template-columns:3fr 1fr">
			<div class="w3-container"><p>1fr</p></div>
			<div class="w3-container">
				<div id="chart_div"></div>
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
