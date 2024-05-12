import React, { Component, PropTypes } from 'react';
import { Panel, ListGroup, ListGroupItem, Badge, Label,
        OverlayTrigger, Tooltip, Button, ButtonToolbar, Popover } from 'react-bootstrap';
import Carousel, { Item as CarouselItem } from 'react-bootstrap/lib/Carousel';
import proImage1 from '../../../../common/images/img1.jpg';
import proImage2 from '../../../../common/images/img2.jpg';
import proImage3 from '../../../../common/images/img3.jpg';

const title = 'Other Elements';

class OtherElements extends Component { // eslint-disable-line

  static contextTypes = {
    setTitle: PropTypes.func.isRequired,
  };

  componentWillMount() {
    this.context.setTitle(title);
  }
  render() {
    return (
      <div className="row animate">
        <div className="col-lg-8 col-md-6">
          <Panel
            header={<span>List Group </span>}
            bsStyle="primary"
          >
            <ListGroup>
              <ListGroupItem>Cras justo odio</ListGroupItem>
              <ListGroupItem>Dapibus ac facilisis in</ListGroupItem>
              <ListGroupItem>Morbi leo risus</ListGroupItem>
              <ListGroupItem>Porta ac consectetur ac</ListGroupItem>
              <ListGroupItem>Vestibulum at eros</ListGroupItem>
            </ListGroup>

            <ListGroup>
              <ListGroupItem>
                Cras justo odio
                <Badge>14</Badge>
              </ListGroupItem>
            </ListGroup>

            <ListGroup>
              <ListGroupItem active>Cras justo odio</ListGroupItem>
              <ListGroupItem>Dapibus ac facilisis in</ListGroupItem>
              <ListGroupItem>Morbi leo risus</ListGroupItem>
              <ListGroupItem>Porta ac consectetur ac</ListGroupItem>
              <ListGroupItem>Vestibulum at eros</ListGroupItem>
            </ListGroup>

            <ListGroup>
              <ListGroupItem bsStyle="success">Dapibus ac facilisis in</ListGroupItem>
              <ListGroupItem bsStyle="info">Morbi leo risus</ListGroupItem>
              <ListGroupItem bsStyle="warning">Porta ac consectetur ac</ListGroupItem>
              <ListGroupItem bsStyle="danger">Vestibulum at eros</ListGroupItem>
            </ListGroup>

            <ListGroup>
              <ListGroupItem header="List group item heading" active>
                Lorem ipsum dolor sit amet, consectetur adipisicing elit. Ratione officiis
                totam eos optio!
              </ListGroupItem>
              <ListGroupItem header="List group item heading">
                  Lorem ipsum dolor sit amet, consectetur adipisicing elit. Ratione officiis
                  totam eos optio!
              </ListGroupItem>
              <ListGroupItem header="List group item heading">
                  Lorem ipsum dolor sit amet, consectetur adipisicing elit. Ratione officiis
                  totam eos optio!
              </ListGroupItem>
            </ListGroup>
          </Panel>
        </div>

        <div className="col-lg-4 col-md-6">
          <Panel
            header={<span>Labels &amp; Badges</span>}
            bsStyle="warning"
          >
            <Label bsStyle="default">labels</Label>&nbsp;
            <Label bsStyle="primary">labels</Label>&nbsp;
            <Label bsStyle="success">labels</Label>&nbsp;
            <Label bsStyle="info">labels</Label>&nbsp;
            <Label bsStyle="warning">labels</Label>&nbsp;
            <Label bsStyle="danger">labels</Label>
            <br /><br />
            <Badge>35</Badge>&nbsp;
            <Badge>67</Badge>&nbsp;
            <Badge>12</Badge>&nbsp;
            <Badge>39</Badge>&nbsp;
            <Badge>07</Badge>&nbsp;
            <Badge>94</Badge>&nbsp;
            <Badge><i className="fa fa-star" /></Badge>&nbsp;
            <Badge><i className="fa fa-certificate" /></Badge>
          </Panel>

          <Panel
            header={<span>Carousel</span>}
            bsStyle="warning"
            className="text-carousel bg-primary"
          >
            <Carousel controls={false}>
              <CarouselItem>
                Success is not final, failure is not fatal: it is the courage to continue
                that counts.
              </CarouselItem>
              <CarouselItem>
                To succeed in life, you need two things: ignorance and confidence.
              </CarouselItem>
              <CarouselItem>
                Success is how high you bounce when you hit bottom.
              </CarouselItem>
            </Carousel>
          </Panel>

          <Panel
            header={<span>Carousel Image Silder</span>}
            bsStyle="info"
          >
            <Carousel>
              <CarouselItem>
                <img width="100%" height={200} alt="proPic1" src={proImage1} />
              </CarouselItem>
              <CarouselItem>
                <img width="100%" height={200} alt="proIma2" src={proImage2} />
              </CarouselItem>
              <CarouselItem>
                <img width="100%" height={200} alt="proIma3" src={proImage3} />
              </CarouselItem>
            </Carousel>
          </Panel>

          <Panel
            header={<span>Tooltip &amp; Popover</span>}
            bsStyle="info"
          >
            <div>
              <ButtonToolbar>
                <OverlayTrigger
                  placement="left" overlay={<Tooltip id="tooltip-1"> On the Left! </Tooltip>}
                >
                  <Button> Left Tooltip </Button>
                </OverlayTrigger>
              &nbsp;
                <OverlayTrigger
                  placement="top" overlay={<Tooltip id="tooltip-2"> On the Top! </Tooltip>}
                >
                  <Button> Top Tooltip </Button>
                </OverlayTrigger>
              &nbsp;
                <OverlayTrigger
                  placement="bottom" overlay={<Tooltip id="tooltip-3"> On the Bottom! </Tooltip>}
                >
                  <Button> Bottom Tooltip </Button>
                </OverlayTrigger>
              &nbsp;
                <OverlayTrigger
                  placement="right" overlay={<Tooltip id="tooltip-4"> On the Right! </Tooltip>}
                >
                  <Button> Right Tooltip </Button>
                </OverlayTrigger>
              &nbsp;
              </ButtonToolbar>
              <br />
              <div>
                <ButtonToolbar>
                  <OverlayTrigger
                    trigger="click"
                    placement="left"
                    overlay={<Popover id="popover-1"><strong>On the Left!</strong></Popover>}
                  >
                    <Button>Left Popover</Button>
                  </OverlayTrigger>
                  <OverlayTrigger
                    trigger="click"
                    placement="top"
                    overlay={<Popover id="popover-2"><strong>On the Top!</strong></Popover>}
                  >
                    <Button>Top Popover</Button>
                  </OverlayTrigger>
                  <OverlayTrigger
                    trigger="click"
                    placement="bottom"
                    overlay={<Popover id="popover-3"><strong>On the Bottom!</strong></Popover>}
                  >
                    <Button>Bottom Popover</Button>
                  </OverlayTrigger>
                  <OverlayTrigger
                    trigger="click"
                    placement="right"
                    overlay={<Popover id="popover-4"><strong>On the Right!</strong></Popover>}
                  >
                    <Button>Right Popover</Button>
                  </OverlayTrigger>
                </ButtonToolbar>
              </div>
            </div>
          </Panel>
        </div>
      </div>
    );
  }
}

export default OtherElements;
