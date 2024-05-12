

import React, { PropTypes, Component } from 'react';
import withStyles from 'isomorphic-style-loader/lib/withStyles';
import { Panel } from 'react-bootstrap';
import { FormattedMessage } from 'react-intl';
import s from './Home.css';
import Stats from '../../components/common/Stats';
import TodoList from '../../components/common/TodoList';
import {
  LineChart, Tooltip, XAxis, YAxis, CartesianGrid, Line as LineRechart, AreaChart, Area,
  BarChart, Bar, ResponsiveContainer } from '../../vendor/recharts';

import CustomPieChart from '../../components/CustomPieChart';


const title = 'Ani React';

function plotData() {
  const data = [];
  const offset = 0;
  let sineValue;
  let cosValue;
  for (let i = 0; i < 12; i += 0.8) {
    sineValue = Math.sin(i + offset);
    cosValue = Math.cos(i + offset);
    data.push({ name: i, sine: sineValue, cosine: cosValue });
    // data.push({ name: i, cosine: cosValue });
  }
  return data;
}
const lineChartData = plotData();


const areaData = [
      { name: 'Page A', uv: 4000, pv: 2400, amt: 2400, value: 600 },
      { name: 'Page B', uv: 3000, pv: 1398, amt: 2210, value: 300 },
      { name: 'Page C', uv: 2000, pv: 9800, amt: 2290, value: 500 },
      { name: 'Page D', uv: 2780, pv: 3908, amt: 2000, value: 400 },
      { name: 'Page E', uv: 1890, pv: 4800, amt: 2181, value: 200 },
      { name: 'Page F', uv: 2390, pv: 3800, amt: 2500, value: 700 },
      { name: 'Page G', uv: 3490, pv: 1300, amt: 2100, value: 100 },
      { name: 'Page H', uv: 3490, pv: 3300, amt: 2100, value: 200 },
      { name: 'Page I', uv: 2490, pv: 2300, amt: 2100, value: 300 },
      { name: 'Page J', uv: 5490, pv: 2800, amt: 2100, value: 700 },
      { name: 'Page G', uv: 4490, pv: 3800, amt: 2100, value: 400 },
      { name: 'Page H', uv: 2490, pv: 1300, amt: 2100, value: 300 },
      { name: 'Page I', uv: 5490, pv: 4300, amt: 2100, value: 500 },
      { name: 'Page J', uv: 2490, pv: 3300, amt: 2100, value: 100 },
      { name: 'Page K', uv: 4490, pv: 2300, amt: 2100, value: 500 },
];


// const pieData01 = [{ name: 'Group A', value: 400 }, { name: 'Group B', value: 300 },
//                   { name: 'Group C', value: 300 }, { name: 'Group D', value: 200 }];


class Home extends Component {

  static contextTypes = {
    setTitle: PropTypes.func.isRequired,
  };
  componentWillMount() {
    this.context.setTitle(title);
  }

  render() {
    return (
      <div className="animate">
        <div className="row">
          <div className="col-md-4 col-lg-3">
            <div className={s.homeStats}>

              <Stats
                icon="cloud-upload"
                text={
                  <FormattedMessage
                    id="stat1"
                    defaultMessage="Server Uptime"
                  />
              }
                bgClass="info"
                link="/dashboard/chartjs"
                value="88%"
                progressValue={88}
              />
              <Stats
                icon="heartbeat"
                text={
                  <FormattedMessage
                    id="stat2"
                    defaultMessage="Positive feedback"
                  />
              }
                bgClass="success"
                link="/dashboard/chartjs"
                value="94%"
                progressValue={94}
              />
              <Stats
                icon="flag"

                text={
                  <FormattedMessage
                    id="stat3"
                    defaultMessage="Posts flagged"
                  />
                }
                bgClass="danger"
                link="/dashboard/inbox"
                value="12,351"
                progressValue={72}
              />

            </div>
          </div>

          <div className="col-md-5 col-lg-6">
            <div className="map-container box padder">
              <div className="panel">
                <div className="panel-body">
                  <ResponsiveContainer width="100%" height={254}>
                    <AreaChart
                      data={areaData} margin={{ top: 10, right: 30, left: -15, bottom: 0 }}
                    >
                      <XAxis dataKey="name" />
                      <YAxis />
                      <CartesianGrid stroke="#ccc" />
                      <Tooltip />
                      <Area
                        type="monotone"
                        dataKey="uv"
                        stackId="1"
                        stroke="#8884d8"
                        fill="#8884d8"
                      />
                      <Area
                        type="monotone"
                        dataKey="pv"
                        stackId="1"
                        stroke="#82ca9d"
                        fill="#82ca9d"
                      />
                      <Area
                        type="monotone"
                        dataKey="amt"
                        stackId="1"
                        stroke="#ffc658"
                        fill="#ffc658"
                      />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>
              </div>
            </div>
          </div>

          <div className="col-md-3 col-lg-3">
            <div className="row">
              <div className="col-sm-12">
                <div className={`${s.topRightChart}`}>
                  <ResponsiveContainer width="100%" height={142}>
                    <BarChart data={areaData} margin={{ top: 10, right: 10, left: 10, bottom: 0 }} >
                      <Tooltip />
                      <Bar dataKey="value" fill="#DE6764" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>
            </div>
            <div className={'row'}>
              <div className="col-sm-12 text-left">
                <div className={`${s.bottomRightChart}`}>
                  <div className="row">
                    <div className="col-sm-3 col-xs-5 text-left">
                      <span className="heading">
                        <FormattedMessage
                          id="views"
                          defaultMessage="Total Views"
                        />
                      </span>
                      <br />
                      <big className="text-primary">22068</big>
                    </div>
                    <div className="col-sm-9 col-xs-7">
                      <ResponsiveContainer height={100}>

                        <CustomPieChart />

                      </ResponsiveContainer>

                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>

        </div>

        <div className="row">


          <div className="col-md-8 col-lg-8">
            <div className={`map-container box padder ${s.powerSupplyChart}`}>
              <Panel header={<span>Power Supply Waves</span>}>
                <div>
                  <ResponsiveContainer width="100%" height={275}>
                    <LineChart data={lineChartData}>
                      <CartesianGrid stroke="#ccc" />
                      <XAxis />
                      <YAxis />
                      <Tooltip />
                      <LineRechart type="monotone" dataKey="sine" stroke="#8884d8" />
                      <LineRechart type="monotone" dataKey="cosine" stroke="#82ca9d" />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </Panel>
            </div>
          </div>

          <div className="col-md-4 col-lg-4">
            <TodoList />
          </div>
        </div>
      </div>
    );
  }
}


export default withStyles(s)(Home);
