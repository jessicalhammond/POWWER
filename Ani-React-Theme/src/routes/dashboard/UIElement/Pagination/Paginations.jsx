import React, { Component, PropTypes } from 'react';
import { Panel, Pagination } from 'react-bootstrap';
import Breadcrumb, { Item as BreadcrumbItem } from 'react-bootstrap/lib/Breadcrumb';
import Pager, { Item as PagerItem } from 'react-bootstrap/lib/Pager';

const title = 'Pagination';

class Paginations extends Component {
  static contextTypes = {
    setTitle: PropTypes.func.isRequired,
  };
  constructor(props) {
    super(props);
    this.state = {
      activePage1: 1,
      activePage2: 4,
      activePage3: 3,
    };
    this.handleSelect1 = this.handleSelect1.bind(this);
    this.handleSelect2 = this.handleSelect2.bind(this);
    this.handleSelect3 = this.handleSelect3.bind(this);
  }


  componentWillMount() {
    this.context.setTitle(title);
  }
  handleSelect1(selectedEvent) {
    this.setState({
      activePage1: selectedEvent,
    });
  }
  handleSelect2(selectedEvent) {
    this.setState({
      activePage2: selectedEvent,
    });
  }
  handleSelect3(selectedEvent) {
    this.setState({
      activePage3: selectedEvent,
    });
  }

  render() {
    return (
      <div className="animate">
        <div className="row">
          <div className="col-md-6">
            <Panel
              header={<span> Breadcrumb </span>}
              bsStyle="success"
            >
              <div className="clearfix">
                <Breadcrumb>
                  <BreadcrumbItem active>
                    <span className="fa fa-home" />
                  </BreadcrumbItem>
                  <BreadcrumbItem href="" onClick={(e) => { e.preventDefault(); }}>
                    Library
                  </BreadcrumbItem>
                  <BreadcrumbItem href="" onClick={(e) => { e.preventDefault(); }}>
                    Data
                  </BreadcrumbItem>
                </Breadcrumb>
              </div>

              <div className="clearfix">
                <Breadcrumb className="pull-right">
                  <BreadcrumbItem href="" onClick={(e) => { e.preventDefault(); }}>
                    <span className="fa fa-home" />
                  </BreadcrumbItem>
                  <BreadcrumbItem active>
                    Library
                  </BreadcrumbItem>
                  <BreadcrumbItem href="" onClick={(e) => { e.preventDefault(); }}>
                    Data
                  </BreadcrumbItem>
                </Breadcrumb>
              </div>

              <div className="clearfix">
                <Breadcrumb>
                  <BreadcrumbItem active>
                    <span className="fa fa-home" />
                  </BreadcrumbItem>
                  <BreadcrumbItem href="" onClick={(e) => { e.preventDefault(); }}>
                    Library
                  </BreadcrumbItem>
                  <BreadcrumbItem href="" onClick={(e) => { e.preventDefault(); }}>
                    Data
                  </BreadcrumbItem>
                </Breadcrumb>
              </div>
            </Panel>

            <Panel
              header={<span>Pager</span>}
              bsStyle="warning"
            >
              <Pager>
                <PagerItem href="" onClick={(e) => { e.preventDefault(); }}>Previous</PagerItem>
                <PagerItem href="" onClick={(e) => { e.preventDefault(); }}>Next</PagerItem>
              </Pager>
              <Pager>
                <PagerItem previous href="" onClick={(e) => { e.preventDefault(); }}>
                  <i className="fa fa-angle-double-left" /> Previous
                </PagerItem>
                <PagerItem next href="" onClick={(e) => { e.preventDefault(); }}>
                  Next <i className="fa fa-angle-double-right" />
                </PagerItem>
              </Pager>
            </Panel>
          </div>

          <div className="col-md-6">
            <Panel
              header={<span>Pagination</span>}
              bsStyle="info"
            >
              <div>
                <Pagination
                  prev
                  next
                  first
                  last
                  activePage={this.state.activePage1}
                  items={20}
                  maxButtons={7}
                  bsSize="small"
                  onSelect={this.handleSelect1}
                />
              </div>

              <div className="text-center">
                <Pagination
                  prev
                  next
                  first
                  last
                  activePage={this.state.activePage2}
                  items={20}
                  maxButtons={7}
                  bsSize="small"
                  onSelect={this.handleSelect2}
                />
              </div>
              <div className="pull-right">
                <Pagination
                  prev
                  next
                  first
                  last
                  activePage={this.state.activePage3}
                  items={20}
                  maxButtons={7}
                  bsSize="small"
                  onSelect={this.handleSelect3}
                />
              </div>
            </Panel>
            <Panel
              header={<span>Pagination Size</span>}
              bsStyle="danger"
            >
              <div>
                <Pagination
                  prev
                  next
                  first
                  last
                  items={20}
                  maxButtons={5}
                  bsSize="large"
                />
              </div>

              <div>
                <Pagination
                  prev
                  next
                  first
                  last
                  items={20}
                  maxButtons={7}
                  bsSize="medium"
                />
              </div>
              <div>
                <Pagination
                  prev
                  next
                  first
                  last
                  items={20}
                  maxButtons={7}
                  bsSize="small"
                />
              </div>
            </Panel>
          </div>
        </div>
      </div>
    );
  }
}

export default Paginations;
