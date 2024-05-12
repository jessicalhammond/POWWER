import React, { Component } from 'react';
import { FormattedMessage } from 'react-intl';
import withStyles from 'isomorphic-style-loader/lib/withStyles';
import $ from 'jquery';
import s from './MenuBar.css';
import Link from '../../Link/Link';
// import Translate from '../../common/Translate';

class MenuBar extends Component {

  constructor(props) {
    super(props);
    this.state = {
      activeState: 0,
    };
    this.handleActiveClass = this.handleActiveClass.bind(this);
  }

  componentDidMount() {
    // window.scrollTo(0, 0);
    // setTimeout(() => {
    //   window.scrollTo(0, 0);
    //   console.log("insdie scroll");
    // }, 1000);
    const that = this;
    const loc = window.location.href;
    const pageIndex = loc.indexOf('/dashboard/');
    let str = null;
    if (pageIndex !== -1) {
      str = loc.substr(pageIndex + 11);
      if (str === 'inbox' || str === 'compose') {
        str = 'mail';
      } else if (str === 'elements' || str === 'components') {
        str = 'form';
      } else if (str === 'buttons' || str === 'alerts' || str === 'dropdowns' || str === 'icons' || str === 'panels' || str === 'progressbars' || str === 'paginations' || str === 'other-elements') {
        str = 'userInterface';
      } else if (str === 'chartjs' || str === 'morrisjs') {
        str = 'charts';
      } else if (str === 'blank') {
        str = 'miscellaneous';
      }
    }
    switch (str) {
      case 'home': that.setState({ activeState: 0 });
        break;
      case 'typography': that.setState({ activeState: 1 });
        break;
      case 'grid': that.setState({ activeState: 2 });
        break;
      case 'table': that.setState({ activeState: 3 });
        break;
      case 'form': that.setState({ activeState: 4 });
        break;
      case 'userInterface': that.setState({ activeState: 5 });
        break;
      case 'chart': that.setState({ activeState: 6 });
        break;
      case 'calendar': that.setState({ activeState: 7 });
        break;
      case 'mail': that.setState({ activeState: 8 });
        break;
      case 'invoice': that.setState({ activeState: 9 });
        break;
      case 'docs': that.setState({ activeState: 10 });
        break;
      case 'miscellaneous': that.setState({ activeState: 11 });
        break;
      default: that.setState({ activeState: 0 });
    }
  }

  handleActiveClass(name) {
    switch (name) {
      case 'home': this.setState({ activeState: 0 });
        break;
      case 'typography': this.setState({ activeState: 1 });
        break;
      case 'grid': this.setState({ activeState: 2 });
        break;
      case 'table': this.setState({ activeState: 3 });
        break;
      case 'form': this.setState({ activeState: 4 });
        break;
      case 'userInterface': this.setState({ activeState: 5 });
        break;
      case 'chart': this.setState({ activeState: 6 });
        break;
      case 'calendar': this.setState({ activeState: 7 });
        break;
      case 'mail': this.setState({ activeState: 8 });
        break;
      case 'invoice': this.setState({ activeState: 9 });
        break;
      case 'docs': this.setState({ activeState: 10 });
        break;
      case 'miscellaneous': this.setState({ activeState: 11 });
        break;
      default: this.setState({ activeState: 0 });
    }
  }
  hideMenu() {
    $('.dashboard-page').toggleClass('push-right');
  }
  render() {
    return (
      <div className={`side-menu ${s.sideMenu}`}>
        <div className={`menu-body ${s.menuBody}`}>
          <ul className={`nav nav-pills nav-stacked no-margin ${s.sidenav}`} onClick={() => this.hideMenu()}>
            <li
              className={this.state.activeState === 0 ? 'active' : ''}
              onClick={() => { this.handleActiveClass('home'); }}
            >
              <Link to="/">
                <span className="glyphicon glyphicon-home" />
              </Link>
              <ul className={`nested-dropdown animated fadeIn ${s.nestedDropdown}`}>
                <li><Link to="/">
                  <FormattedMessage
                    id="dashboard"
                    defaultMessage="Dashboard"
                  /></Link>
                </li>
              </ul>
            </li>
            <li
              className={this.state.activeState === 1 ? 'active' : ''}
              onClick={() => { this.handleActiveClass('typography'); }}
            >
              <Link to="/dashboard/typography">
                <span className="glyphicon glyphicon-text-size" />
              </Link>
              <ul className={`nested-dropdown animated fadeIn ${s.nestedDropdown}`}>
                <li><Link to="/dashboard/typography">
                  <FormattedMessage
                    id="typography"
                    defaultMessage="Typography"
                  /></Link>
                </li>
              </ul>
            </li>
            <li
              className={this.state.activeState === 2 ? 'active' : ''}
              onClick={(e) => { e.preventDefault(); this.handleActiveClass('grid'); }}
            >
              <Link to="/dashboard/grid">
                <span className="glyphicon glyphicon-th-large" />
              </Link>
              <ul className={`nested-dropdown animated fadeIn ${s.nestedDropdown}`}>
                <li>
                  <Link to="/dashboard/grid">
                    <FormattedMessage
                      id="grid"
                      defaultMessage="Grid"
                    />
                  </Link>
                </li>
              </ul>
            </li>
            <li
              className={this.state.activeState === 3 ? 'active' : ''}
              onClick={() => { this.handleActiveClass('table'); }}
            >
              <Link to="/dashboard/table">
                <span className="glyphicon glyphicon-list" />
              </Link>
              <ul className={`nested-dropdown animated fadeIn ${s.nestedDropdown}`}>
                <li>
                  <Link to="/dashboard/table">
                    <FormattedMessage
                      id="table"
                      defaultMessage="Table"
                    />
                  </Link>
                </li>
              </ul>
            </li>
            <li
              className={this.state.activeState === 4 ? 'active' : ''}
              onClick={() => { this.handleActiveClass('form'); }}
            >
              <Link to="/dashboard/elements">
                <span className="glyphicon glyphicon-equalizer" />
              </Link>
              <ul className={`nested-dropdown animated fadeIn ${s.nestedDropdown}`}>
                <li
                  className={`sidemenu-header ${s.sideMenuHeader}`}
                  onClick={() => { this.handleActiveClass('form'); }}

                >
                  <FormattedMessage
                    id="form"
                    defaultMessage="Form"
                  />
                </li>
                <li
                  onClick={() => { this.handleActiveClass('form'); }}
                >
                  <Link to="/dashboard/elements">
                    <FormattedMessage
                      id="elements"
                      defaultMessage="Form Elements"
                    />
                  </Link>
                </li>
                <li
                  onClick={() => { this.handleActiveClass('form'); }}
                >
                  <Link to="/dashboard/components">
                    <FormattedMessage
                      id="components"
                      defaultMessage="Form Components"
                    />
                  </Link>
                </li>
              </ul>
            </li>
            <li
              className={this.state.activeState === 5 ? 'active' : ''}
              onClick={() => { this.handleActiveClass('userInterface'); }}
            >
              <Link to="/dashboard/buttons">
                <span className="glyphicon glyphicon-cloud-download" />
              </Link>
              <ul className={`nested-dropdown animated fadeIn ${s.nestedDropdown}`}>
                <li className={`sidemenu-header ${s.sideMenuHeader}`}>
                  <FormattedMessage
                    id="user_interface"
                    defaultMessage="User Interface"
                  />
                </li>
                <li><Link to="/dashboard/buttons">
                  <FormattedMessage
                    id="buttons"
                    defaultMessage="Buttons"
                  />
                </Link></li>
                <li><Link to="/dashboard/dropdowns">
                  <FormattedMessage
                    id="dropdown"
                    defaultMessage="DropDown"
                  />
                </Link></li>
                <li><Link to="/dashboard/icons">
                  <FormattedMessage
                    id="icons"
                    defaultMessage="Icons"
                  />
                </Link></li>
                <li><Link to="/dashboard/panels">
                  <FormattedMessage
                    id="panels"
                    defaultMessage="Panels"
                  />
                </Link></li>
                <li><Link to="/dashboard/alerts">
                  <FormattedMessage
                    id="alerts"
                    defaultMessage="Alerts"
                  />
                </Link></li>
                <li><Link to="/dashboard/progressbars">
                  <FormattedMessage
                    id="progressbars"
                    defaultMessage="Progress Bars"
                  />
                </Link></li>
                <li><Link to="/dashboard/paginations">
                  <FormattedMessage
                    id="pagination"
                    defaultMessage="Pagination"
                  />
                </Link></li>
                <li><Link to="/dashboard/other-elements">
                  <FormattedMessage
                    id="other_elements"
                    defaultMessage="Other Elements"
                  />
                </Link></li>
              </ul>
            </li>
            <li
              className={this.state.activeState === 6 ? 'active' : ''}
              onClick={() => { this.handleActiveClass('chart'); }}
            >
              <Link to="/dashboard/chartjs">
                <span className="glyphicon glyphicon-signal" />
              </Link>
              <ul className={`nested-dropdown animated fadeIn ${s.nestedDropdown}`}>
                <li className={`sidemenu-header ${s.sideMenuHeader}`}>
                  <FormattedMessage
                    id="charts"
                    defaultMessage="Charts"
                  />
                </li>
                <li><Link to="/dashboard/chartjs">
                  <FormattedMessage
                    id="chartjs"
                    defaultMessage="chart Js"
                  /></Link></li>
                <li><Link to="/dashboard/morris">
                  <FormattedMessage
                    id="morrisjs"
                    defaultMessage="Morris Chart"
                  />
                </Link></li>
              </ul>
            </li>
            <li
              className={this.state.activeState === 7 ? 'active' : ''}
              onClick={() => { this.handleActiveClass('calendar'); }}
            >
              <Link to="/dashboard/calendar">
                <span className="glyphicon glyphicon-calendar" />
              </Link>
              <ul className={`nested-dropdown animated fadeIn ${s.nestedDropdown}`}>
                <li><Link to="/dashboard/calendar">
                  <FormattedMessage
                    id="calendar"
                    defaultMessage="Calendar"
                  />
                </Link></li>
              </ul>
            </li>
            <li
              className={this.state.activeState === 8 ? 'active' : ''}
              onClick={() => { this.handleActiveClass('mail'); }}
            >
              <Link to="/dashboard/inbox">
                <span className="glyphicon glyphicon-envelope" />
              </Link>
              <ul className={`nested-dropdown animated fadeIn ${s.nestedDropdown}`}>
                <li className={`sidemenu-header ${s.sideMenuHeader}`}>
                  <FormattedMessage
                    id="mail"
                    defaultMessage="Mail"
                  />
                </li>
                <li><Link to="/dashboard/inbox">
                  <FormattedMessage
                    id="inbox"
                    defaultMessage="Inbox"
                  />
                </Link></li>
                <li><Link to="/dashboard/compose">
                  <FormattedMessage
                    id="compose"
                    defaultMessage="Compose"
                  />
                </Link></li>
              </ul>
            </li>
            <li
              className={this.state.activeState === 9 ? 'active' : ''}
              onClick={() => { this.handleActiveClass('invoice'); }}
            >
              <Link to="/dashboard/invoice">
                <span className="glyphicon glyphicon-list-alt" />
              </Link>
              <ul className={`nested-dropdown animated fadeIn ${s.nestedDropdown}`}>
                <li><Link to="/dashboard/invoice">
                  <FormattedMessage
                    id="invoice"
                    defaultMessage="Invoice"
                  />
                </Link></li>
              </ul>
            </li>
            <li
              className={this.state.activeState === 10 ? 'active' : ''}
              onClick={() => { this.handleActiveClass('docs'); }}
            >
              <a href="http://www.strapui.com/docs/ani-reactjs-theme/v1.0.0/overview/introduction">
                <span className="glyphicon glyphicon-credit-card" />
              </a>
              <ul className={`nested-dropdown animated fadeIn ${s.nestedDropdown}`}>
                <li><a href="http://www.strapui.com/docs/ani-reactjs-theme/v1.0.0/overview/introduction">
                  <FormattedMessage
                    id="docs"
                    defaultMessage="Docs"
                  />
                </a></li>
              </ul>
            </li>
            <li
              className={this.state.activeState === 11 ? 'active' : ''}
              onClick={() => { this.handleActiveClass('miscellaneous'); }}
            >
              <Link to="/dashboard/blank">
                <span className="glyphicon glyphicon-duplicate" />
              </Link>
              <ul className={`nested-dropdown animated fadeIn ${s.nestedDropdown}`}>
                <li className={`sidemenu-header ${s.sideMenuHeader}`}>
                  <FormattedMessage
                    id="pages"
                    defaultMessage="Pages"
                  />
                </li>
                <li><Link to="/dashboard/blank">
                  <FormattedMessage
                    id="blankpage"
                    defaultMessage="Blank Page"
                  />
                </Link></li>
                <li><Link to="/login">
                  <FormattedMessage
                    id="login"
                    defaultMessage="Login"
                  />
                </Link></li>
                <li><Link to="/signup">
                  <FormattedMessage
                    id="signup"
                    defaultMessage="Sign Up"
                  />
                </Link></li>
                <li><Link to="error-page">
                  <FormattedMessage
                    id="404page"
                    defaultMessage="404 Page"
                  /></Link></li>
              </ul>
            </li>
          </ul>
        </div>
      </div>
    );
  }
}

export default withStyles(s)(MenuBar);
